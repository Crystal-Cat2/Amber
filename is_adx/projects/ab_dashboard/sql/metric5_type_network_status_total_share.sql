-- 查询目的：
-- 统计每个 network_type + network 在全部 request 分母下的四状态占比：
-- AD_LOADED / FAILED_TO_LOAD / AD_LOAD_NOT_ATTEMPTED / NULL
--
-- 输出字段：
-- experiment_group, product, ad_format, network_type, network, status_bucket,
-- request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一请求按 user_pseudo_id + request_id 定义
-- 2. 分母是当前 experiment_group + product + ad_format 下的全部 request 总数
-- 3. 行维度是 network_type + network
-- 4. 同一个 request + type + network 如果存在多个状态，按优先级归并为唯一最终状态：
--    AD_LOADED > FAILED_TO_LOAD > AD_LOAD_NOT_ATTEMPTED > NULL
-- 5. NULL 定义为：当前 request 中，该 network_type + network 没有任何记录

WITH
-- 1. 全部 request 分母
request_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS denominator_request_pv
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
  GROUP BY experiment_group, product, ad_format
),

-- 2. 三种真实状态的原始明细
base AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, 'unknown')) AS network_type,
    network,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, 'unknown')) IN ('bidding', 'waterfall')
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

-- 3. 同一 product + ad_format 下实际出现过的 type + network
network_universe AS (
  SELECT DISTINCT
    product,
    ad_format,
    network_type,
    network
  FROM base
),

-- 4. 扩到 A/B 两组，保证两组都有同一套行
group_networks AS (
  SELECT
    t.experiment_group,
    t.product,
    t.ad_format,
    u.network_type,
    u.network
  FROM request_totals t
  JOIN network_universe u
    ON t.product = u.product
   AND t.ad_format = u.ad_format
),

-- 5. 每个 request + type + network 的最终状态，按优先级压成唯一状态
final_status AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network_type,
    network,
    CASE
      WHEN MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOADED'
      WHEN MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) = 1 THEN 'FAILED_TO_LOAD'
      WHEN MAX(CASE WHEN status = 'AD_LOAD_NOT_ATTEMPTED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOAD_NOT_ATTEMPTED'
    END AS status_bucket
  FROM base
  GROUP BY
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network_type,
    network
),

-- 6. 三种真实状态的 request 数
status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    network_type,
    network,
    status_bucket,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS request_pv
  FROM final_status
  GROUP BY
    experiment_group,
    product,
    ad_format,
    network_type,
    network,
    status_bucket
),

-- 7. NULL 的 request 数
null_counts AS (
  SELECT
    g.experiment_group,
    g.product,
    g.ad_format,
    g.network_type,
    g.network,
    'NULL' AS status_bucket,
    t.denominator_request_pv
      - COUNT(DISTINCT CONCAT(f.user_pseudo_id, '||', f.request_id)) AS request_pv
  FROM group_networks g
  JOIN request_totals t
    ON g.experiment_group = t.experiment_group
   AND g.product = t.product
   AND g.ad_format = t.ad_format
  LEFT JOIN final_status f
    ON g.experiment_group = f.experiment_group
   AND g.product = f.product
   AND g.ad_format = f.ad_format
   AND g.network_type = f.network_type
   AND g.network = f.network
  GROUP BY
    g.experiment_group,
    g.product,
    g.ad_format,
    g.network_type,
    g.network,
    t.denominator_request_pv
)

SELECT
  x.experiment_group,
  x.product,
  x.ad_format,
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
JOIN request_totals t
  ON x.experiment_group = t.experiment_group
 AND x.product = t.product
 AND x.ad_format = t.ad_format
ORDER BY
  x.product,
  x.ad_format,
  x.network_type,
  x.network,
  CASE x.status_bucket
    WHEN 'AD_LOADED' THEN 1
    WHEN 'FAILED_TO_LOAD' THEN 2
    WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 3
    WHEN 'NULL' THEN 4
    ELSE 99
  END;
