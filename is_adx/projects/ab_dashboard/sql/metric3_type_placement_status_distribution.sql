-- 查询目的：固定 total placement_cnt、network_type 与 type_placement_cnt 后，统计全量 / 有成功 / 无成功 request 的 status 分布。
-- 输出字段：experiment_group, product, ad_format, success_scope, placement_cnt, network_type, type_placement_cnt, status_bucket, request_pv, denominator_request_pv, share
-- 关键口径：
-- 1. 统计对象是全量去重 request，不再按请求顺序编号，也不限制前 200 次。
-- 2. total placement_cnt 与 type_placement_cnt 都按 placement 行数统计，不去重 placement；只统计 bidding / waterfall。
-- 3. placement 侧不做 waterfall 优先级归并，同一 request 若命中多个 placement status，可同时落入多个 status_bucket。
-- 4. request 级成功定义：同一 request 内存在任一 status = 'AD_LOADED'。
-- 5. 每个 status_bucket 的 request_pv 仍按去重 request_id 统计，避免同一 request 在同一状态下重复累计。

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    placement_id,
    COALESCE(status, 'NULL_STATUS') AS status
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
    placement_id,
    status
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
    COUNT(*) AS placement_cnt
  FROM request_success_scopes s
  INNER JOIN request_placement_detail d
    ON s.experiment_group = d.experiment_group
    AND s.product = d.product
    AND s.ad_format = d.ad_format
    AND s.user_pseudo_id = d.user_pseudo_id
    AND s.request_id = d.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
),

type_request_counts AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
    p.user_pseudo_id,
    p.request_id,
    p.placement_cnt,
    d.network_type,
    COUNT(*) AS type_placement_cnt
  FROM per_request_counts p
  INNER JOIN request_placement_detail d
    ON p.experiment_group = d.experiment_group
    AND p.product = d.product
    AND p.ad_format = d.ad_format
    AND p.user_pseudo_id = d.user_pseudo_id
    AND p.request_id = d.request_id
  GROUP BY p.experiment_group, p.product, p.ad_format, p.success_scope, p.user_pseudo_id, p.request_id, p.placement_cnt, d.network_type
),

request_status_bucket AS (
  SELECT DISTINCT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.success_scope,
    t.placement_cnt,
    t.network_type,
    t.type_placement_cnt,
    t.user_pseudo_id,
    t.request_id,
    d.status AS status_bucket
  FROM type_request_counts t
  INNER JOIN request_placement_detail d
    ON t.experiment_group = d.experiment_group
    AND t.product = d.product
    AND t.ad_format = d.ad_format
    AND t.user_pseudo_id = d.user_pseudo_id
    AND t.request_id = d.request_id
    AND t.network_type = d.network_type
  WHERE d.status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    network_type,
    type_placement_cnt,
    COUNT(DISTINCT request_id) AS denominator_request_pv
  FROM type_request_counts
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt, network_type, type_placement_cnt
),

status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    network_type,
    type_placement_cnt,
    status_bucket,
    COUNT(DISTINCT request_id) AS request_pv
  FROM request_status_bucket
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt, network_type, type_placement_cnt, status_bucket
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.success_scope,
  c.placement_cnt,
  c.network_type,
  c.type_placement_cnt,
  c.status_bucket,
  c.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(c.request_pv, d.denominator_request_pv) AS share
FROM status_counts c
INNER JOIN denominator_totals d
  ON c.experiment_group = d.experiment_group
  AND c.product = d.product
  AND c.ad_format = d.ad_format
  AND c.success_scope = d.success_scope
  AND c.placement_cnt = d.placement_cnt
  AND c.network_type = d.network_type
  AND c.type_placement_cnt = d.type_placement_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.success_scope,
  c.experiment_group,
  c.placement_cnt,
  c.network_type,
  c.type_placement_cnt,
  CASE c.status_bucket
    WHEN 'AD_LOADED' THEN 1
    WHEN 'FAILED_TO_LOAD' THEN 2
    WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 3
    ELSE 99
  END;
