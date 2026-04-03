-- 查询目的：
-- 统计每个 experiment_group + product + ad_format + max_unit_id 下，
-- 单个 request 缺失了多少个 bidding 渠道（NULL），以及每个缺失个数对应的去重 request PV。
--
-- 输出字段：
-- experiment_group, product, ad_format, max_unit_id, status_bucket, bidding_cnt, request_pv,
-- denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一 request 按 product + user_pseudo_id + request_id 定义
-- 2. NULL 不是 status IS NULL，而是当前 request + bidding network 在明细里没有记录
-- 3. 先按 request + network 把真实三种状态压成唯一最终状态
-- 4. 再用 request × bidding network 全集直接补出 NULL
-- 5. 输出包含 bidding_cnt = 0 在内的完整 NULL 分布

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
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

network_universe AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    network
  FROM bidding_status_base
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
      WHEN MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) = 1 THEN 'AD_LOADED'
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

request_network_status AS (
  SELECT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.max_unit_id,
    r.request_key,
    u.network,
    COALESCE(f.status_bucket, 'NULL') AS status_bucket
  FROM request_base r
  INNER JOIN network_universe u
    ON r.experiment_group = u.experiment_group
   AND r.product = u.product
   AND r.ad_format = u.ad_format
   AND r.max_unit_id = u.max_unit_id
  LEFT JOIN final_status f
    ON r.experiment_group = f.experiment_group
   AND r.product = f.product
   AND r.ad_format = f.ad_format
   AND r.max_unit_id = f.max_unit_id
   AND r.request_key = f.request_key
   AND u.network = f.network
),

request_null_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key,
    COUNTIF(status_bucket = 'NULL') AS null_bidding_cnt
  FROM request_network_status
  GROUP BY
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_key
)

SELECT
  n.experiment_group,
  n.product,
  n.ad_format,
  n.max_unit_id,
  'NULL' AS status_bucket,
  n.null_bidding_cnt AS bidding_cnt,
  COUNT(*) AS request_pv,
  t.denominator_request_pv,
  SAFE_DIVIDE(COUNT(*), t.denominator_request_pv) AS share
FROM request_null_counts n
INNER JOIN request_totals t
  ON n.experiment_group = t.experiment_group
 AND n.product = t.product
 AND n.ad_format = t.ad_format
 AND n.max_unit_id = t.max_unit_id
GROUP BY
  n.experiment_group,
  n.product,
  n.ad_format,
  n.max_unit_id,
  status_bucket,
  bidding_cnt,
  t.denominator_request_pv
ORDER BY
  n.experiment_group,
  n.product,
  n.ad_format,
  n.max_unit_id,
  bidding_cnt;
