-- 查询目的：固定 placement_cnt 与 Bx+Wy 结构后，按 status 下钻各 placement_id 的 request 命中占比。
-- 输出字段：experiment_group, product, ad_format, success_scope, placement_cnt, bidding_placement_cnt, waterfall_placement_cnt, status_bucket, placement_id, request_pv, denominator_request_pv, share

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    placement_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    COALESCE(status, 'NULL') AS status_bucket
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
),

request_status_placement AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    status_bucket,
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
    MAX(CASE WHEN status_bucket = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_ad_loaded
  FROM request_status_placement
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

per_request_bucket AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    s.success_scope,
    s.user_pseudo_id,
    s.request_id,
    COUNT(*) AS placement_cnt,
    COUNTIF(b.network_type = 'bidding') AS bidding_placement_cnt,
    COUNTIF(b.network_type = 'waterfall') AS waterfall_placement_cnt
  FROM request_success_scopes s
  INNER JOIN base_latency b
    ON s.experiment_group = b.experiment_group
    AND s.product = b.product
    AND s.ad_format = b.ad_format
    AND s.user_pseudo_id = b.user_pseudo_id
    AND s.request_id = b.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    bidding_placement_cnt,
    waterfall_placement_cnt,
    COUNT(*) AS denominator_request_pv
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt, bidding_placement_cnt, waterfall_placement_cnt
),

target_counts AS (
  SELECT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.success_scope,
    b.placement_cnt,
    b.bidding_placement_cnt,
    b.waterfall_placement_cnt,
    t.status_bucket,
    t.placement_id,
    COUNT(*) AS request_pv
  FROM per_request_bucket b
  INNER JOIN request_status_placement t
    ON b.experiment_group = t.experiment_group
    AND b.product = t.product
    AND b.ad_format = t.ad_format
    AND b.user_pseudo_id = t.user_pseudo_id
    AND b.request_id = t.request_id
  GROUP BY
    b.experiment_group,
    b.product,
    b.ad_format,
    b.success_scope,
    b.placement_cnt,
    b.bidding_placement_cnt,
    b.waterfall_placement_cnt,
    t.status_bucket,
    t.placement_id
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.success_scope,
  c.placement_cnt,
  c.bidding_placement_cnt,
  c.waterfall_placement_cnt,
  c.status_bucket,
  c.placement_id,
  c.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(c.request_pv, d.denominator_request_pv) AS share
FROM target_counts c
INNER JOIN denominator_totals d
  ON c.experiment_group = d.experiment_group
  AND c.product = d.product
  AND c.ad_format = d.ad_format
  AND c.success_scope = d.success_scope
  AND c.placement_cnt = d.placement_cnt
  AND c.bidding_placement_cnt = d.bidding_placement_cnt
  AND c.waterfall_placement_cnt = d.waterfall_placement_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.success_scope,
  c.placement_cnt,
  c.bidding_placement_cnt,
  c.waterfall_placement_cnt,
  c.status_bucket,
  c.experiment_group,
  c.request_pv DESC,
  c.placement_id;

