-- 查询目的：
-- 统计“某个 type + network 是唯一 AD_LOADED 胜利渠道”时，
-- 其他渠道在这些 request 上的状态命中率，输出到 max_unit_id 粒度。
--
-- 输出字段：
-- experiment_group, product, ad_format, winner_network_type, winner_network,
-- max_unit_id, network_type, network, status_bucket,
-- request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一 request 按 product + user_pseudo_id + request_id 定义
-- 2. 胜利渠道 = 当前 request 内唯一 AD_LOADED 的 type + network
-- 3. 分母 = 当前 winner type + network + unit 下满足条件的 request 总数
-- 4. 分子 = 这些 request 上其他渠道命中当前 status 的 request 数
-- 5. share 是命中率，不是互斥状态分布，因此各状态相加不要求等于 100%
-- 6. NULL 只统计 bidding；waterfall 不统计 NULL

WITH request_detail AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    CONCAT(product, '||', user_pseudo_id, '||', request_id) AS request_key,
    LOWER(COALESCE(network_type, 'unknown')) AS network_type,
    network,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND max_unit_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, 'unknown')) IN ('bidding', 'waterfall')
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

winner_requests AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    SPLIT(MIN(IF(status = 'AD_LOADED', CONCAT(network_type, '||', network), NULL)), '||')[OFFSET(0)] AS winner_network_type,
    SPLIT(MIN(IF(status = 'AD_LOADED', CONCAT(network_type, '||', network), NULL)), '||')[OFFSET(1)] AS winner_network
  FROM request_detail
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key
  HAVING COUNT(DISTINCT IF(status = 'AD_LOADED', CONCAT(network_type, '||', network), NULL)) = 1
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    COUNT(DISTINCT request_key) AS denominator_request_pv
  FROM winner_requests
  GROUP BY
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id
),

other_real_status AS (
  SELECT DISTINCT
    w.experiment_group,
    w.product,
    w.ad_format,
    w.winner_network_type,
    w.winner_network,
    w.max_unit_id,
    d.request_key,
    d.network_type,
    d.network,
    d.status AS status_bucket
  FROM winner_requests w
  INNER JOIN request_detail d
    ON w.experiment_group = d.experiment_group
   AND w.product = d.product
   AND w.ad_format = d.ad_format
   AND w.max_unit_id = d.max_unit_id
   AND w.request_key = d.request_key
  WHERE NOT (
    d.network_type = w.winner_network_type
    AND d.network = w.winner_network
  )
),

real_status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    network_type,
    network,
    status_bucket,
    COUNT(DISTINCT request_key) AS request_pv
  FROM other_real_status
  GROUP BY
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    network_type,
    network,
    status_bucket
),

bidding_network_universe AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    network
  FROM other_real_status
  WHERE network_type = 'bidding'
),

bidding_present_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    network,
    COUNT(DISTINCT request_key) AS present_request_pv
  FROM other_real_status
  WHERE network_type = 'bidding'
  GROUP BY
    experiment_group,
    product,
    ad_format,
    winner_network_type,
    winner_network,
    max_unit_id,
    network
),

null_status_counts AS (
  SELECT
    u.experiment_group,
    u.product,
    u.ad_format,
    u.winner_network_type,
    u.winner_network,
    u.max_unit_id,
    'bidding' AS network_type,
    u.network,
    'NULL' AS status_bucket,
    d.denominator_request_pv - COALESCE(p.present_request_pv, 0) AS request_pv
  FROM bidding_network_universe u
  INNER JOIN denominator_totals d
    ON u.experiment_group = d.experiment_group
   AND u.product = d.product
   AND u.ad_format = d.ad_format
   AND u.winner_network_type = d.winner_network_type
   AND u.winner_network = d.winner_network
   AND u.max_unit_id = d.max_unit_id
  LEFT JOIN bidding_present_counts p
    ON u.experiment_group = p.experiment_group
   AND u.product = p.product
   AND u.ad_format = p.ad_format
   AND u.winner_network_type = p.winner_network_type
   AND u.winner_network = p.winner_network
   AND u.max_unit_id = p.max_unit_id
   AND u.network = p.network
)

SELECT
  x.experiment_group,
  x.product,
  x.ad_format,
  x.winner_network_type,
  x.winner_network,
  x.max_unit_id,
  x.network_type,
  x.network,
  x.status_bucket,
  x.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(x.request_pv, d.denominator_request_pv) AS share
FROM (
  SELECT * FROM real_status_counts
  UNION ALL
  SELECT * FROM null_status_counts
) x
INNER JOIN denominator_totals d
  ON x.experiment_group = d.experiment_group
 AND x.product = d.product
 AND x.ad_format = d.ad_format
 AND x.winner_network_type = d.winner_network_type
 AND x.winner_network = d.winner_network
 AND x.max_unit_id = d.max_unit_id
ORDER BY
  x.product,
  x.ad_format,
  x.winner_network_type,
  x.winner_network,
  x.max_unit_id,
  CASE x.network_type
    WHEN 'bidding' THEN 1
    WHEN 'waterfall' THEN 2
    ELSE 99
  END,
  x.network,
  CASE x.status_bucket
    WHEN 'AD_LOADED' THEN 1
    WHEN 'FAILED_TO_LOAD' THEN 2
    WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 3
    WHEN 'NULL' THEN 4
    ELSE 99
  END;
