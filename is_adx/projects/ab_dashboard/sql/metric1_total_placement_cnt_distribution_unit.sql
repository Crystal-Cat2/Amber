-- 查询目的：统计全量 request 的总 placement_cnt 分布，并新增 max_unit_id 维度。
-- 输出字段：experiment_group, product, ad_format, max_unit_id, placement_cnt, request_pv, denominator_request_pv, share

WITH request_placement_detail AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    placement_id
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

per_request_bucket AS (
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

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    COUNT(DISTINCT request_id) AS denominator_request_pv
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, max_unit_id
),

bucket_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    placement_cnt,
    COUNT(DISTINCT request_id) AS request_pv
  FROM per_request_bucket
  GROUP BY experiment_group, product, ad_format, max_unit_id, placement_cnt
)

SELECT
  b.experiment_group,
  b.product,
  b.ad_format,
  b.max_unit_id,
  b.placement_cnt,
  b.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(b.request_pv, d.denominator_request_pv) AS share
FROM bucket_counts b
INNER JOIN denominator_totals d
  ON b.experiment_group = d.experiment_group
  AND b.product = d.product
  AND b.ad_format = d.ad_format
  AND b.max_unit_id = d.max_unit_id
ORDER BY
  b.product,
  b.ad_format,
  b.max_unit_id,
  b.experiment_group,
  b.placement_cnt;
