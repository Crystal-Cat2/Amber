-- 查询目的：输出指标3结果，观察在单一 status + network_type + network_cnt 条件下，具体 network 的请求分布。
-- 输出字段：experiment_group, product, ad_format, req_index, status, network_type, network_cnt, network, pv_count, bucket_request_cnt, share_in_bucket
-- 关键口径：同一 request 内同一 network 多条日志只记 1 次，请求先定桶，再看桶内具体 network 分布。
WITH
-- 先定义请求粒度，并限制当前只看 interstitial 与 rewarded。
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

-- 对每个用户在同一 product + ad_format 内的请求做顺序编号。
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

-- 回连原始日志，并保留指标3需要的状态与 network_type。
request_detail AS (
  SELECT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.user_pseudo_id,
    r.request_id,
    r.req_index,
    COALESCE(d.status, 'NULL_STATUS') AS status,
    LOWER(COALESCE(d.network_type, 'unknown')) AS network_type,
    d.network
  FROM ranked_requests r
  INNER JOIN `commercial-adx.lmh.isadx_adslog_latency_detail` d
    ON r.experiment_group = d.experiment_group
    AND r.product = d.product
    AND r.ad_format = d.ad_format
    AND r.user_pseudo_id = d.user_pseudo_id
    AND r.request_id = d.request_id
  WHERE r.req_index <= 1000
    AND d.request_id IS NOT NULL
    AND d.network IS NOT NULL
),

-- 指标3只看三种状态与两种类型，并在 request 内去重 network。
selected_request_network AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    req_index,
    user_pseudo_id,
    request_id,
    status,
    network_type,
    network
  FROM request_detail
  WHERE status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
    AND network_type IN ('bidding', 'waterfall')
),

-- 先把每个 request 在单一 status + network_type 条件下的 network_cnt 算出来。
per_request_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    user_pseudo_id,
    request_id,
    status,
    network_type,
    COUNT(*) AS network_cnt
  FROM selected_request_network
  GROUP BY experiment_group, product, ad_format, req_index, user_pseudo_id, request_id, status, network_type
),

-- 再看每个桶里，具体有哪些 network 覆盖到了这些 request。
bucket_network_pv AS (
  SELECT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.req_index,
    b.status,
    b.network_type,
    b.network_cnt,
    n.network,
    COUNT(*) AS pv_count
  FROM per_request_bucket b
  INNER JOIN selected_request_network n
    ON b.experiment_group = n.experiment_group
    AND b.product = n.product
    AND b.ad_format = n.ad_format
    AND b.req_index = n.req_index
    AND b.user_pseudo_id = n.user_pseudo_id
    AND b.request_id = n.request_id
    AND b.status = n.status
    AND b.network_type = n.network_type
  GROUP BY b.experiment_group, b.product, b.ad_format, b.req_index, b.status, b.network_type, b.network_cnt, n.network
),

-- 单独保留每个桶内的 request 总数，方便页面同时展示桶规模。
bucket_request_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    status,
    network_type,
    network_cnt,
    COUNT(*) AS bucket_request_cnt
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, req_index, status, network_type, network_cnt
)

-- 最终输出具体 network 的 request 分布，以及其在桶内的占比。
SELECT
  p.experiment_group,
  p.product,
  p.ad_format,
  p.req_index,
  p.status,
  p.network_type,
  p.network_cnt,
  p.network,
  p.pv_count,
  t.bucket_request_cnt,
  SAFE_DIVIDE(
    p.pv_count,
    SUM(p.pv_count) OVER (
      PARTITION BY p.experiment_group, p.product, p.ad_format, p.req_index, p.status, p.network_type, p.network_cnt
    )
  ) AS share_in_bucket
FROM bucket_network_pv p
INNER JOIN bucket_request_totals t
  ON p.experiment_group = t.experiment_group
  AND p.product = t.product
  AND p.ad_format = t.ad_format
  AND p.req_index = t.req_index
  AND p.status = t.status
  AND p.network_type = t.network_type
  AND p.network_cnt = t.network_cnt
ORDER BY p.experiment_group, p.product, p.ad_format, p.req_index, p.status, p.network_type, p.network_cnt, p.network;
