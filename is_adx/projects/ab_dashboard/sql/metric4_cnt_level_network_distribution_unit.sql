-- 查询目的：按 max_unit_id 固定 network_cnt 与 Bx+Wy 结构后，按 status 下钻各 B/W-network 的 request 命中占比。

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
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
    AND max_unit_id IS NOT NULL
),
typed_request_network AS (
  SELECT DISTINCT experiment_group, product, ad_format, max_unit_id, user_pseudo_id, request_id, network_type, network
  FROM base_latency
),
typed_request_status_network AS (
  SELECT DISTINCT experiment_group, product, ad_format, max_unit_id, user_pseudo_id, request_id, status_bucket, network_type, network
  FROM base_latency
),
per_request_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(network_type, network))) AS network_cnt,
    COUNT(DISTINCT IF(network_type = 'bidding', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS bidding_cnt,
    COUNT(DISTINCT IF(network_type = 'waterfall', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS waterfall_cnt
  FROM typed_request_network
  GROUP BY experiment_group, product, ad_format, max_unit_id, user_pseudo_id, request_id
),
denominator_totals AS (
  SELECT experiment_group, product, ad_format, max_unit_id, network_cnt, bidding_cnt, waterfall_cnt, COUNT(*) AS denominator_request_pv
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, max_unit_id, network_cnt, bidding_cnt, waterfall_cnt
),
target_counts AS (
  SELECT
    b.experiment_group,
    b.product,
    b.ad_format,
    b.max_unit_id,
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
    AND b.max_unit_id = t.max_unit_id
    AND b.user_pseudo_id = t.user_pseudo_id
    AND b.request_id = t.request_id
  GROUP BY b.experiment_group, b.product, b.ad_format, b.max_unit_id, b.network_cnt, b.bidding_cnt, b.waterfall_cnt, t.status_bucket, t.network_type, t.network
)
SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.max_unit_id,
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
  AND c.max_unit_id = d.max_unit_id
  AND c.network_cnt = d.network_cnt
  AND c.bidding_cnt = d.bidding_cnt
  AND c.waterfall_cnt = d.waterfall_cnt
ORDER BY c.product, c.ad_format, c.max_unit_id, c.network_cnt, c.bidding_cnt, c.waterfall_cnt, c.status_bucket, c.experiment_group, c.request_pv DESC, c.network_type, c.network;

