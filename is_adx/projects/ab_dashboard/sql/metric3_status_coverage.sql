-- 查询目的：固定 req_index + network_cnt 后，统计三种状态在当前请求桶中的覆盖率。
-- 输出字段：experiment_group, product, ad_format, req_index, network_cnt, status, pv_count, bucket_request_pv, coverage
-- 关键口径：
-- 1. 唯一请求按 user_pseudo_id + request_id 定义
-- 2. network_cnt 按单次请求内去重后的 network 数量计算，不按 placement 计算
-- 3. coverage = 命中过当前 status 的请求数 / 当前 req_index + network_cnt 桶的请求总数
-- 4. 同一请求可能命中多个 status，因此 coverage 加和可能大于 100%
WITH
request_level AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    MIN(event_timestamp) AS request_ts
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

ranked_requests AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    ROW_NUMBER() OVER (
      PARTITION BY experiment_group, product, ad_format, user_pseudo_id
      ORDER BY request_ts, request_id
    ) AS req_index
  FROM request_level
),

request_detail AS (
  SELECT DISTINCT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.req_index,
    r.user_pseudo_id,
    r.request_id,
    COALESCE(d.status, 'NULL_STATUS') AS status,
    d.network
  FROM ranked_requests r
  INNER JOIN `commercial-adx.lmh.isadx_adslog_latency_detail` d
    ON r.experiment_group = d.experiment_group
    AND r.product = d.product
    AND r.ad_format = d.ad_format
    AND r.user_pseudo_id = d.user_pseudo_id
    AND r.request_id = d.request_id
  WHERE r.req_index <= 200
    AND d.request_id IS NOT NULL
    AND d.network IS NOT NULL
    AND d.network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(d.network_type, 'unknown')) IN ('bidding', 'waterfall')
    AND COALESCE(d.status, 'NULL_STATUS') IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

request_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT network) AS network_cnt
  FROM request_detail
  GROUP BY experiment_group, product, ad_format, req_index, user_pseudo_id, request_id
),

bucket_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    network_cnt,
    COUNT(*) AS bucket_request_pv
  FROM request_bucket
  GROUP BY experiment_group, product, ad_format, req_index, network_cnt
),

request_hits_status AS (
  SELECT DISTINCT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.req_index,
    b.network_cnt,
    b.user_pseudo_id,
    b.request_id,
    d.status
  FROM request_bucket b
  INNER JOIN request_detail d
    ON b.experiment_group = d.experiment_group
    AND b.product = d.product
    AND b.ad_format = d.ad_format
    AND b.req_index = d.req_index
    AND b.user_pseudo_id = d.user_pseudo_id
    AND b.request_id = d.request_id
),

coverage_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    network_cnt,
    status,
    COUNT(*) AS pv_count
  FROM request_hits_status
  GROUP BY experiment_group, product, ad_format, req_index, network_cnt, status
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.req_index,
  c.network_cnt,
  c.status,
  c.pv_count,
  b.bucket_request_pv,
  SAFE_DIVIDE(c.pv_count, b.bucket_request_pv) AS coverage
FROM coverage_counts c
INNER JOIN bucket_totals b
  ON c.experiment_group = b.experiment_group
  AND c.product = b.product
  AND c.ad_format = b.ad_format
  AND c.req_index = b.req_index
  AND c.network_cnt = b.network_cnt
ORDER BY c.experiment_group, c.product, c.ad_format, c.req_index, c.network_cnt, c.status;
