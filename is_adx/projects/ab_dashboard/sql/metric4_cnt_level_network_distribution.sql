-- 查询目的：固定 network_cnt 与 Bx+Wy 结构后，按 status 下钻各 B/W-network 的 request 命中占比。
-- 输出字段：experiment_group, product, ad_format, success_scope, network_cnt, bidding_cnt, waterfall_cnt, status_bucket, network_type, network, request_pv, denominator_request_pv, share

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    network,
    COALESCE(status, 'NULL') AS status_bucket
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

typed_request_network AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network_type,
    network
  FROM base_latency
),

typed_request_status_network AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    status_bucket,
    network_type,
    network
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
  FROM typed_request_status_network
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
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(t.network_type, t.network))) AS network_cnt,
    COUNT(DISTINCT IF(t.network_type = 'bidding', TO_JSON_STRING(STRUCT(t.network_type, t.network)), NULL)) AS bidding_cnt,
    COUNT(DISTINCT IF(t.network_type = 'waterfall', TO_JSON_STRING(STRUCT(t.network_type, t.network)), NULL)) AS waterfall_cnt
  FROM request_success_scopes s
  INNER JOIN typed_request_network t
    ON s.experiment_group = t.experiment_group
    AND s.product = t.product
    AND s.ad_format = t.ad_format
    AND s.user_pseudo_id = t.user_pseudo_id
    AND s.request_id = t.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    network_cnt,
    bidding_cnt,
    waterfall_cnt,
    COUNT(*) AS denominator_request_pv
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, success_scope, network_cnt, bidding_cnt, waterfall_cnt
),

target_counts AS (
  SELECT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.success_scope,
    b.network_cnt,
    b.bidding_cnt,
    b.waterfall_cnt,
    t.status_bucket,
    t.network_type,
    t.network,
    COUNT(*) AS request_pv
  FROM per_request_bucket b
  INNER JOIN typed_request_status_network t
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
    b.network_cnt,
    b.bidding_cnt,
    b.waterfall_cnt,
    t.status_bucket,
    t.network_type,
    t.network
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.success_scope,
  c.network_cnt,
  c.bidding_cnt,
  c.waterfall_cnt,
  c.status_bucket,
  c.network_type,
  c.network,
  c.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(c.request_pv, d.denominator_request_pv) AS share
FROM target_counts c
INNER JOIN denominator_totals d
  ON c.experiment_group = d.experiment_group
  AND c.product = d.product
  AND c.ad_format = d.ad_format
  AND c.success_scope = d.success_scope
  AND c.network_cnt = d.network_cnt
  AND c.bidding_cnt = d.bidding_cnt
  AND c.waterfall_cnt = d.waterfall_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.success_scope,
  c.network_cnt,
  c.bidding_cnt,
  c.waterfall_cnt,
  c.status_bucket,
  c.experiment_group,
  c.request_pv DESC,
  c.network_type,
  c.network;
