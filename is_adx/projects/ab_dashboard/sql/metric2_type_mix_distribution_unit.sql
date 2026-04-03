-- 查询目的：固定总 network_cnt 后，统计 bidding_cnt + waterfall_cnt 的组合分布，并新增 max_unit_id 维度。
-- 输出字段：experiment_group, product, ad_format, max_unit_id, network_cnt, bidding_cnt, waterfall_cnt, request_pv, denominator_request_pv, share

WITH typed_request_network AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    user_pseudo_id,
    request_id,
    network,
    LOWER(COALESCE(network_type, '')) AS network_type
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND max_unit_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

per_request_counts AS (
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
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    network_cnt,
    COUNT(*) AS denominator_request_pv
  FROM per_request_counts
  GROUP BY experiment_group, product, ad_format, max_unit_id, network_cnt
),

bucket_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    max_unit_id,
    network_cnt,
    bidding_cnt,
    waterfall_cnt,
    COUNT(*) AS request_pv
  FROM per_request_counts
  GROUP BY experiment_group, product, ad_format, max_unit_id, network_cnt, bidding_cnt, waterfall_cnt
)

SELECT
  b.experiment_group,
  b.product,
  b.ad_format,
  b.max_unit_id,
  b.network_cnt,
  b.bidding_cnt,
  b.waterfall_cnt,
  b.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(b.request_pv, d.denominator_request_pv) AS share
FROM bucket_counts b
INNER JOIN denominator_totals d
  ON b.experiment_group = d.experiment_group
  AND b.product = d.product
  AND b.ad_format = d.ad_format
  AND b.max_unit_id = d.max_unit_id
  AND b.network_cnt = d.network_cnt
ORDER BY
  b.product,
  b.ad_format,
  b.max_unit_id,
  b.experiment_group,
  b.network_cnt,
  b.bidding_cnt,
  b.waterfall_cnt;
