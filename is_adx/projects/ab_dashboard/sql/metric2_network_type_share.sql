-- 查询目的：输出指标2结果，分别观察 bidding 与 waterfall 在不同状态组合下的去重 network 个数分布。
-- 输出字段：experiment_group, product, ad_format, req_index, network_type, status_selection, network_cnt, pv_count
-- 关键口径：同一 request 内，同一 network 在同一 network_type 下即使跨多个被选状态重复出现，也只能算 1 个。
WITH
-- 先枚举页面会用到的全部状态多选组合。
status_selection_map AS (
  SELECT 'AD_LOADED' AS status_selection, ['AD_LOADED'] AS status_list UNION ALL
  SELECT 'FAILED_TO_LOAD', ['FAILED_TO_LOAD'] UNION ALL
  SELECT 'AD_LOAD_NOT_ATTEMPTED', ['AD_LOAD_NOT_ATTEMPTED'] UNION ALL
  SELECT 'AD_LOADED+FAILED_TO_LOAD', ['AD_LOADED', 'FAILED_TO_LOAD'] UNION ALL
  SELECT 'AD_LOADED+AD_LOAD_NOT_ATTEMPTED', ['AD_LOADED', 'AD_LOAD_NOT_ATTEMPTED'] UNION ALL
  SELECT 'FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED', ['FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED'] UNION ALL
  SELECT 'AD_LOADED+FAILED_TO_LOAD+AD_LOAD_NOT_ATTEMPTED', ['AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED']
),

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

-- 回连原始日志，并规范化 network_type；同时只保留前 200 次请求，并先排除不参与本次统计的 TpAdxCustomAdapter。
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
  WHERE r.req_index <= 200
    AND d.request_id IS NOT NULL
    AND d.network IS NOT NULL
    AND d.network != 'TpAdxCustomAdapter'
),

-- 只保留指标2当前会展示的两类 network_type，并在每个状态组合下先做 request 内去重。
selected_request_network AS (
  SELECT DISTINCT
    d.network_type,
    s.status_selection,
    d.experiment_group,
    d.product,
    d.ad_format,
    d.req_index,
    d.user_pseudo_id,
    d.request_id,
    d.network
  FROM request_detail d
  INNER JOIN status_selection_map s
    ON d.status IN UNNEST(s.status_list)
  WHERE d.network_type IN ('bidding', 'waterfall')
),

-- 对每个 request、每个 network_type、每个状态组合分别计算去重 network 个数。
per_request_bucket AS (
  SELECT
    network_type,
    status_selection,
    experiment_group,
    product,
    ad_format,
    req_index,
    user_pseudo_id,
    request_id,
    COUNT(*) AS network_cnt
  FROM selected_request_network
  GROUP BY network_type, status_selection, experiment_group, product, ad_format, req_index, user_pseudo_id, request_id
)

-- 最终输出 bidding / waterfall 各自的 network_cnt 分布。
SELECT
  experiment_group,
  product,
  ad_format,
  req_index,
  network_type,
  status_selection,
  network_cnt,
  COUNT(*) AS pv_count
FROM per_request_bucket
GROUP BY experiment_group, product, ad_format, req_index, network_type, status_selection, network_cnt
ORDER BY experiment_group, product, ad_format, req_index, network_type, status_selection, network_cnt;
