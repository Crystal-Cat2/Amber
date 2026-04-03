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

-- 对每个用户在同一 product + ad_format 内的请求做顺序编号，只保留前 200 次请求。
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

-- 回连原始日志，并保留指标3需要的状态与 network_type；同时先排除不参与本次统计的 TpAdxCustomAdapter。
request_detail AS (
  SELECT DISTINCT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.req_index,
    r.user_pseudo_id,
    r.request_id,
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
  WHERE r.req_index <= 200
    AND d.request_id IS NOT NULL
    AND d.network IS NOT NULL
    AND d.network != 'TpAdxCustomAdapter'
    AND COALESCE(d.status, 'NULL_STATUS') IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
    AND LOWER(COALESCE(d.network_type, 'unknown')) IN ('bidding', 'waterfall')
),

-- 先在单一 status + network_type 下按 request 计算去重后的 network_cnt。
request_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    status,
    network_type,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT network) AS network_cnt
  FROM request_detail
  GROUP BY experiment_group, product, ad_format, req_index, status, network_type, user_pseudo_id, request_id
),

-- 再把已定桶的 request 回连到具体 network，保持 request 内 network 去重。
bucket_network AS (
  SELECT DISTINCT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.req_index,
    b.status,
    b.network_type,
    b.network_cnt,
    b.user_pseudo_id,
    b.request_id,
    d.network
  FROM request_bucket b
  INNER JOIN request_detail d
    ON b.experiment_group = d.experiment_group
    AND b.product = d.product
    AND b.ad_format = d.ad_format
    AND b.req_index = d.req_index
    AND b.user_pseudo_id = d.user_pseudo_id
    AND b.request_id = d.request_id
    AND b.status = d.status
    AND b.network_type = d.network_type
),

-- 每个桶里先统计 request 总数，便于后续计算桶内 share。
bucket_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    status,
    network_type,
    network_cnt,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS bucket_request_cnt
  FROM request_bucket
  GROUP BY experiment_group, product, ad_format, req_index, status, network_type, network_cnt
),

-- 统计每个桶里各 network 覆盖到的 request 数。
network_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    req_index,
    status,
    network_type,
    network_cnt,
    network,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS pv_count
  FROM bucket_network
  GROUP BY experiment_group, product, ad_format, req_index, status, network_type, network_cnt, network
)
SELECT
  n.experiment_group,
  n.product,
  n.ad_format,
  n.req_index,
  n.status,
  n.network_type,
  n.network_cnt,
  n.network,
  n.pv_count,
  t.bucket_request_cnt,
  SAFE_DIVIDE(n.pv_count, t.bucket_request_cnt) AS share_in_bucket
FROM network_counts n
INNER JOIN bucket_totals t
  ON n.experiment_group = t.experiment_group
  AND n.product = t.product
  AND n.ad_format = t.ad_format
  AND n.req_index = t.req_index
  AND n.status = t.status
  AND n.network_type = t.network_type
  AND n.network_cnt = t.network_cnt
ORDER BY
  n.product,
  n.ad_format,
  n.req_index,
  n.status,
  n.network_type,
  n.network_cnt,
  n.pv_count DESC,
  n.network;
