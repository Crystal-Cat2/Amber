-- 查询目的：
-- 统计每个 experiment_group + product + ad_format + max_unit_id + status_bucket 下，
-- 单个 request 命中了多少个该状态的 bidding 渠道，以及每个个数对应的去重 request PV。
--
-- 输出字段：
-- experiment_group, product, ad_format, max_unit_id, status_bucket, bidding_cnt,
-- request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一 request 按 product + user_pseudo_id + request_id 定义
-- 2. 先按 request + network 把真实三种状态压成唯一最终状态
-- 3. 再按 request + status_bucket 统计该 request 命中了多少个该状态渠道
-- 4. 只输出 FAILED_TO_LOAD / AD_LOAD_NOT_ATTEMPTED 两种真实状态
-- 5. 输出包含 bidding_cnt = 0 在内的完整状态分布

WITH request_base AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    CONCAT(product, '||', user_pseudo_id, '||', request_id) AS request_key
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE request_id IS NOT NULL
    AND ad_format IS NOT NULL
    AND max_unit_id IS NOT NULL
),

bidding_status_base AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    CONCAT(product, '||', user_pseudo_id, '||', request_id) AS request_key,
    network,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE request_id IS NOT NULL
    AND ad_format IS NOT NULL
    AND max_unit_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, 'unknown')) = 'bidding'
    AND status IN ('FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

final_status AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    network,
    CASE
      WHEN MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) = 1 THEN 'FAILED_TO_LOAD'
      WHEN MAX(CASE WHEN status = 'AD_LOAD_NOT_ATTEMPTED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOAD_NOT_ATTEMPTED'
    END AS status_bucket
  FROM bidding_status_base
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    network
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

status_options AS (
  SELECT 'FAILED_TO_LOAD' AS status_bucket
  UNION ALL
  SELECT 'AD_LOAD_NOT_ATTEMPTED' AS status_bucket
),

request_status_counts AS (
  SELECT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.max_unit_id,
    r.request_key,
    s.status_bucket,
    COUNTIF(f.status_bucket = s.status_bucket) AS bidding_cnt
  FROM request_base r
  CROSS JOIN status_options s
  LEFT JOIN final_status f
    ON r.experiment_group = f.experiment_group
   AND r.product = f.product
   AND r.ad_format = f.ad_format
   AND r.max_unit_id = f.max_unit_id
   AND r.request_key = f.request_key
  GROUP BY
    r.experiment_group,
    r.product,
    r.ad_format,
    r.max_unit_id,
    r.request_key,
    s.status_bucket
)

SELECT
  s.experiment_group,
  s.product,
  s.ad_format,
  s.max_unit_id,
  s.status_bucket,
  s.bidding_cnt,
  COUNT(*) AS request_pv,
  t.denominator_request_pv,
  SAFE_DIVIDE(COUNT(*), t.denominator_request_pv) AS share
FROM request_status_counts s
INNER JOIN request_totals t
  ON s.experiment_group = t.experiment_group
 AND s.product = t.product
 AND s.ad_format = t.ad_format
 AND s.max_unit_id = t.max_unit_id
GROUP BY
  s.experiment_group,
  s.product,
  s.ad_format,
  s.max_unit_id,
  s.status_bucket,
  bidding_cnt,
  t.denominator_request_pv
ORDER BY
  s.experiment_group,
  s.product,
  s.ad_format,
  s.max_unit_id,
  CASE s.status_bucket
    WHEN 'FAILED_TO_LOAD' THEN 1
    WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 2
    ELSE 99
  END,
  bidding_cnt;
