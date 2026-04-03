-- 查询目的：固定总 placement_cnt 后，统计全量 / 有成功 / 无成功 request 的 bidding_placement_cnt + waterfall_placement_cnt 组合分布。
-- 输出字段：experiment_group, product, ad_format, success_scope, placement_cnt, bidding_placement_cnt, waterfall_placement_cnt, request_pv, denominator_request_pv, share
-- 关键口径：
-- 1. 统计对象是全量去重 request，不再按请求顺序编号，也不限制前 200 次。
-- 2. 同一 request 内，total placement_cnt、bidding_placement_cnt、waterfall_placement_cnt 都按 placement 行数统计，不去重 placement。
-- 3. request 级成功定义：同一 request 内存在任一 status = 'AD_LOADED'。
-- 4. share = 当前 total placement_cnt + success_scope 桶内，该组合的 request_pv / 当前 total placement_cnt 桶总 request_pv。

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    placement_id,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

request_placement_detail AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network_type,
    placement_id
  FROM base_latency
),

per_request_success AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_ad_loaded
  FROM base_latency
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

request_success_scopes AS (
  SELECT experiment_group, product, ad_format, user_pseudo_id, request_id, 'all' AS success_scope
  FROM per_request_success
  UNION ALL
  SELECT experiment_group, product, ad_format, user_pseudo_id, request_id, 'has_success' AS success_scope
  FROM per_request_success
  WHERE has_ad_loaded = 1
  UNION ALL
  SELECT experiment_group, product, ad_format, user_pseudo_id, request_id, 'no_success' AS success_scope
  FROM per_request_success
  WHERE has_ad_loaded = 0
),

per_request_counts AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    s.success_scope,
    s.user_pseudo_id,
    s.request_id,
    COUNT(*) AS placement_cnt,
    COUNTIF(network_type = 'bidding') AS bidding_placement_cnt,
    COUNTIF(network_type = 'waterfall') AS waterfall_placement_cnt
  FROM request_success_scopes s
  INNER JOIN request_placement_detail d
    ON s.experiment_group = d.experiment_group
    AND s.product = d.product
    AND s.ad_format = d.ad_format
    AND s.user_pseudo_id = d.user_pseudo_id
    AND s.request_id = d.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    COUNT(DISTINCT request_id) AS denominator_request_pv
  FROM per_request_counts
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt
),

bucket_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    bidding_placement_cnt,
    waterfall_placement_cnt,
    COUNT(DISTINCT request_id) AS request_pv
  FROM per_request_counts
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt, bidding_placement_cnt, waterfall_placement_cnt
)

SELECT
  b.experiment_group,
  b.product,
  b.ad_format,
  b.success_scope,
  b.placement_cnt,
  b.bidding_placement_cnt,
  b.waterfall_placement_cnt,
  b.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(b.request_pv, d.denominator_request_pv) AS share
FROM bucket_counts b
INNER JOIN denominator_totals d
  ON b.experiment_group = d.experiment_group
  AND b.product = d.product
  AND b.ad_format = d.ad_format
  AND b.success_scope = d.success_scope
  AND b.placement_cnt = d.placement_cnt
ORDER BY
  b.product,
  b.ad_format,
  b.success_scope,
  b.experiment_group,
  b.placement_cnt,
  b.bidding_placement_cnt,
  b.waterfall_placement_cnt;
