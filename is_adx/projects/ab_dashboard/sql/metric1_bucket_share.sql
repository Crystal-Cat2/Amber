-- 查询目的：统计每个 req_index 下，各 network_cnt 桶的请求数与桶占比。
-- 输出字段：experiment_group, product, ad_format, req_index, network_cnt, pv_count
-- 关键口径：
-- 1. 唯一请求按 user_pseudo_id + request_id 定义
-- 2. network_cnt 按单次请求内去重后的 network 数量计算，不按 placement 计算
-- 3. 本查询只输出桶请求数；桶占比可在下游按同一 req_index 内各桶 pv_count 加总后计算
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

-- 回连原始日志，只保留当前分析会用到的 network / type / status 范围。
request_detail AS (
  SELECT DISTINCT
    r.experiment_group,
    r.product,
    r.ad_format,
    r.req_index,
    r.user_pseudo_id,
    r.request_id,
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

-- 先按请求粒度统计总去重 network 数。
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
)

-- 最终输出每个 req_index 下，各 network_cnt 桶的请求数。
SELECT
  experiment_group,
  product,
  ad_format,
  req_index,
  network_cnt,
  COUNT(*) AS pv_count
FROM request_bucket
GROUP BY experiment_group, product, ad_format, req_index, network_cnt
ORDER BY experiment_group, product, ad_format, req_index, network_cnt;
