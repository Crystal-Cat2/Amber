-- 查询目的：
-- 筛出 bidding 中 AD_LOADED 去重 network 个数为 1 的 request，
-- 再统计这些 request 里其他 bidding-network 在各状态下命中过多少 request，
-- 以及其占满足条件 request 总数的比例。
--
-- 输出字段：
-- experiment_group, product, ad_format, status, network, request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 唯一请求按 user_pseudo_id + request_id 定义
-- 2. 不限制前 200 次请求，统计每个用户的全部请求
-- 3. 只看 bidding
-- 4. 筛选条件：同一 request 下，AD_LOADED 的去重 network 个数 = 1
-- 5. “其他 bidding-network” = 排除掉这 1 个 loaded network 后剩余的 bidding network
-- 6. 同一个 request + network + status 只计 1 次；不同 status 各自计数

WITH request_detail AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COALESCE(status, 'NULL_STATUS') AS status,
    network
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, 'unknown')) = 'bidding'
    AND COALESCE(status, 'NULL_STATUS') IN (
      'AD_LOADED',
      'FAILED_TO_LOAD',
      'AD_LOAD_NOT_ATTEMPTED'
    )
),

loaded_bidding_network AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    MIN(network) AS loaded_network
  FROM request_detail
  WHERE status = 'AD_LOADED'
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
  HAVING COUNT(DISTINCT network) = 1
),

qualified_requests AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    loaded_network
  FROM loaded_bidding_network
),

other_bidding_network_status AS (
  SELECT DISTINCT
    q.experiment_group,
    q.product,
    q.ad_format,
    q.user_pseudo_id,
    q.request_id,
    d.status,
    d.network
  FROM qualified_requests q
  INNER JOIN request_detail d
    ON q.experiment_group = d.experiment_group
    AND q.product = d.product
    AND q.ad_format = d.ad_format
    AND q.user_pseudo_id = d.user_pseudo_id
    AND q.request_id = d.request_id
  WHERE d.network != q.loaded_network
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS denominator_request_pv
  FROM qualified_requests
  GROUP BY experiment_group, product, ad_format
),

network_status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    status,
    network,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '||', request_id)) AS request_pv
  FROM other_bidding_network_status
  GROUP BY experiment_group, product, ad_format, status, network
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.status,
  c.network,
  c.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(c.request_pv, d.denominator_request_pv) AS share
FROM network_status_counts c
INNER JOIN denominator_totals d
  ON c.experiment_group = d.experiment_group
  AND c.product = d.product
  AND c.ad_format = d.ad_format
ORDER BY
  c.product,
  c.ad_format,
  c.status,
  c.request_pv DESC,
  c.network;
