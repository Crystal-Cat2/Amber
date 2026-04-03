-- 查询目的：固定 total network_cnt、network_type 与 type_network_cnt 后，统计三种 status 的分布，并新增 country 维度。
-- 输出字段：experiment_group, product, ad_format, country, network_cnt, network_type, type_network_cnt, status_bucket, request_pv, denominator_request_pv, share

WITH typed_request_network AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    COALESCE(country, 'UNKNOWN') AS country,
    user_pseudo_id,
    request_id,
    network,
    LOWER(COALESCE(network_type, '')) AS network_type
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

per_request_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(network_type, network))) AS network_cnt,
    COUNT(DISTINCT IF(network_type = 'bidding', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS bidding_cnt,
    COUNT(DISTINCT IF(network_type = 'waterfall', TO_JSON_STRING(STRUCT(network_type, network)), NULL)) AS waterfall_cnt
  FROM typed_request_network
  GROUP BY experiment_group, product, ad_format, country, user_pseudo_id, request_id
),

type_request_counts AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.country,
    p.user_pseudo_id,
    p.request_id,
    p.network_cnt,
    r.network_type,
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(r.network_type, r.network))) AS type_network_cnt
  FROM per_request_counts p
  INNER JOIN typed_request_network r
    ON p.experiment_group = r.experiment_group
    AND p.product = r.product
    AND p.ad_format = r.ad_format
    AND p.country = r.country
    AND p.user_pseudo_id = r.user_pseudo_id
    AND p.request_id = r.request_id
  GROUP BY p.experiment_group, p.product, p.ad_format, p.country, p.user_pseudo_id, p.request_id, p.network_cnt, r.network_type
),

request_status_detail AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    COALESCE(country, 'UNKNOWN') AS country,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    COALESCE(status, 'NULL_STATUS') AS status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

bidding_status_requests AS (
  SELECT DISTINCT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.country,
    t.network_cnt,
    t.network_type,
    t.type_network_cnt,
    t.user_pseudo_id,
    t.request_id,
    d.status AS status_bucket
  FROM type_request_counts t
  INNER JOIN request_status_detail d
    ON t.experiment_group = d.experiment_group
    AND t.product = d.product
    AND t.ad_format = d.ad_format
    AND t.country = d.country
    AND t.user_pseudo_id = d.user_pseudo_id
    AND t.request_id = d.request_id
  WHERE t.network_type = 'bidding'
    AND d.network_type = 'bidding'
    AND d.status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

waterfall_status_priority AS (
  SELECT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.country,
    t.network_cnt,
    t.network_type,
    t.type_network_cnt,
    t.user_pseudo_id,
    t.request_id,
    MAX(
      CASE d.status
        WHEN 'AD_LOADED' THEN 3
        WHEN 'FAILED_TO_LOAD' THEN 2
        WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 1
        ELSE 0
      END
    ) AS status_rank
  FROM type_request_counts t
  INNER JOIN request_status_detail d
    ON t.experiment_group = d.experiment_group
    AND t.product = d.product
    AND t.ad_format = d.ad_format
    AND t.country = d.country
    AND t.user_pseudo_id = d.user_pseudo_id
    AND t.request_id = d.request_id
  WHERE t.network_type = 'waterfall'
    AND d.network_type = 'waterfall'
    AND d.status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
  GROUP BY
    t.experiment_group,
    t.product,
    t.ad_format,
    t.country,
    t.network_cnt,
    t.network_type,
    t.type_network_cnt,
    t.user_pseudo_id,
    t.request_id
),

waterfall_status_requests AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    network_cnt,
    network_type,
    type_network_cnt,
    user_pseudo_id,
    request_id,
    CASE status_rank
      WHEN 3 THEN 'AD_LOADED'
      WHEN 2 THEN 'FAILED_TO_LOAD'
      WHEN 1 THEN 'AD_LOAD_NOT_ATTEMPTED'
      ELSE NULL
    END AS status_bucket
  FROM waterfall_status_priority
  WHERE status_rank > 0
),

request_status_bucket AS (
  SELECT * FROM bidding_status_requests
  UNION ALL
  SELECT * FROM waterfall_status_requests
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    network_cnt,
    network_type,
    type_network_cnt,
    COUNT(*) AS denominator_request_pv
  FROM type_request_counts
  GROUP BY experiment_group, product, ad_format, country, network_cnt, network_type, type_network_cnt
),

status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    network_cnt,
    network_type,
    type_network_cnt,
    status_bucket,
    COUNT(*) AS request_pv
  FROM request_status_bucket
  WHERE status_bucket IS NOT NULL
  GROUP BY experiment_group, product, ad_format, country, network_cnt, network_type, type_network_cnt, status_bucket
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.country,
  c.network_cnt,
  c.network_type,
  c.type_network_cnt,
  c.status_bucket,
  c.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(c.request_pv, d.denominator_request_pv) AS share
FROM status_counts c
INNER JOIN denominator_totals d
  ON c.experiment_group = d.experiment_group
  AND c.product = d.product
  AND c.ad_format = d.ad_format
  AND c.country = d.country
  AND c.network_cnt = d.network_cnt
  AND c.network_type = d.network_type
  AND c.type_network_cnt = d.type_network_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.country,
  c.experiment_group,
  c.network_cnt,
  c.network_type,
  c.type_network_cnt,
  CASE c.status_bucket
    WHEN 'AD_LOADED' THEN 1
    WHEN 'FAILED_TO_LOAD' THEN 2
    WHEN 'AD_LOAD_NOT_ATTEMPTED' THEN 3
    ELSE 99
  END;
