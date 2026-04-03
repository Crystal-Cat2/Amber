-- 查询目的：固定 total placement_cnt、network_type 与 type_placement_cnt 后，统计 status 的分布，并新增 max_unit_id 维度。
-- 输出字段：experiment_group, product, ad_format, max_unit_id, placement_cnt, network_type, type_placement_cnt, status_bucket, request_pv, denominator_request_pv, share

WITH request_placement_detail AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    placement_id,
    COALESCE(status, 'NULL_STATUS') AS status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND max_unit_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

per_request_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_id,
    COUNT(*) AS placement_cnt
  FROM request_placement_detail
  GROUP BY experiment_group, product, ad_format, max_unit_id, request_id
),

type_request_counts AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.max_unit_id,
    p.request_id,
    p.placement_cnt,
    d.network_type,
    COUNT(*) AS type_placement_cnt
  FROM per_request_counts p
  INNER JOIN request_placement_detail d
    ON p.experiment_group = d.experiment_group
    AND p.product = d.product
    AND p.ad_format = d.ad_format
    AND p.max_unit_id = d.max_unit_id
    AND p.request_id = d.request_id
  GROUP BY p.experiment_group, p.product, p.ad_format, p.max_unit_id, p.request_id, p.placement_cnt, d.network_type
),

request_status_bucket AS (
  SELECT DISTINCT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.max_unit_id,
    t.placement_cnt,
    t.network_type,
    t.type_placement_cnt,
    t.request_id,
    d.status AS status_bucket
  FROM type_request_counts t
  INNER JOIN request_placement_detail d
    ON t.experiment_group = d.experiment_group
    AND t.product = d.product
    AND t.ad_format = d.ad_format
    AND t.max_unit_id = d.max_unit_id
    AND t.request_id = d.request_id
    AND t.network_type = d.network_type
  WHERE d.status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    placement_cnt,
    network_type,
    type_placement_cnt,
    COUNT(DISTINCT request_id) AS denominator_request_pv
  FROM type_request_counts
  GROUP BY experiment_group, product, ad_format, max_unit_id, placement_cnt, network_type, type_placement_cnt
),

status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    placement_cnt,
    network_type,
    type_placement_cnt,
    status_bucket,
    COUNT(DISTINCT request_id) AS request_pv
  FROM request_status_bucket
  GROUP BY experiment_group, product, ad_format, max_unit_id, placement_cnt, network_type, type_placement_cnt, status_bucket
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.max_unit_id,
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
  AND c.max_unit_id = d.max_unit_id
  AND c.placement_cnt = d.placement_cnt
  AND c.network_type = d.network_type
  AND c.type_placement_cnt = d.type_placement_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.max_unit_id,
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
