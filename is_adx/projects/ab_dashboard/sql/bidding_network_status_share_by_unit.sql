-- 查询目的：
-- 统计每个 experiment_group + product + ad_format + max_unit_id + network_type + network 下，
-- 四种状态（AD_LOADED / FAILED_TO_LOAD / AD_LOAD_NOT_ATTEMPTED / NULL）的 request 占比。
--
-- 输出字段：
-- experiment_group, product, ad_format, max_unit_id, network_type, network, status_bucket,
-- request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一 request 按 product + user_pseudo_id + request_id 定义
-- 2. 只看 bidding / waterfall 渠道，并排除 TpAdxCustomAdapter
-- 3. 分母是当前 experiment_group + product + ad_format + max_unit_id 下的全部 request 总数
-- 4. 同一 request + network_type + network 如果存在多个状态，按优先级归并为唯一最终状态：
--    AD_LOADED > FAILED_TO_LOAD > AD_LOAD_NOT_ATTEMPTED > NULL
-- 5. NULL 定义为：当前 request 下，该 network_type + network 没有任何记录

WITH request_base AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    CONCAT(product, '||', user_pseudo_id, '||', request_id) AS request_key
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND max_unit_id IS NOT NULL
),

request_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    COUNT(DISTINCT request_key) AS denominator_request_pv
  FROM request_base
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id
),

type_network_base AS (
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

network_universe AS (
  SELECT DISTINCT
    product,
    ad_format,
    max_unit_id,
    network_type,
    network
  FROM type_network_base
),

group_networks AS (
  SELECT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.max_unit_id,
    u.network_type,
    u.network
  FROM request_totals t
  INNER JOIN network_universe u
    ON t.product = u.product
   AND t.ad_format = u.ad_format
   AND t.max_unit_id = u.max_unit_id
),

final_status AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    network_type,
    network,
    CASE
      WHEN MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOADED'
      WHEN MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) = 1 THEN 'FAILED_TO_LOAD'
      WHEN MAX(CASE WHEN status = 'AD_LOAD_NOT_ATTEMPTED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOAD_NOT_ATTEMPTED'
    END AS status_bucket
  FROM type_network_base
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    network_type,
    network
),

status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    network_type,
    network,
    status_bucket,
    COUNT(DISTINCT request_key) AS request_pv
  FROM final_status
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    network_type,
    network,
    status_bucket
),

null_counts AS (
  SELECT
    g.experiment_group,
    g.product,
    g.ad_format,
    g.max_unit_id,
    g.network_type,
    g.network,
    'NULL' AS status_bucket,
    t.denominator_request_pv - COUNT(DISTINCT f.request_key) AS request_pv
  FROM group_networks g
  INNER JOIN request_totals t
    ON g.experiment_group = t.experiment_group
   AND g.product = t.product
   AND g.ad_format = t.ad_format
   AND g.max_unit_id = t.max_unit_id
  LEFT JOIN final_status f
    ON g.experiment_group = f.experiment_group
   AND g.product = f.product
   AND g.ad_format = f.ad_format
   AND g.max_unit_id = f.max_unit_id
   AND g.network_type = f.network_type
   AND g.network = f.network
  GROUP BY
    g.experiment_group,
    g.product,
    g.ad_format,
    g.max_unit_id,
    g.network_type,
    g.network,
    t.denominator_request_pv
)

SELECT
  x.experiment_group,
  x.product,
  x.ad_format,
  x.max_unit_id,
  x.network_type,
  x.network,
  x.status_bucket,
  x.request_pv,
  t.denominator_request_pv,
  SAFE_DIVIDE(x.request_pv, t.denominator_request_pv) AS share
FROM (
  SELECT * FROM status_counts
  UNION ALL
  SELECT * FROM null_counts
) x
INNER JOIN request_totals t
  ON x.experiment_group = t.experiment_group
 AND x.product = t.product
 AND x.ad_format = t.ad_format
 AND x.max_unit_id = t.max_unit_id
ORDER BY
  x.experiment_group,
  x.product,
  x.ad_format,
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
