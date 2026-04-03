-- 查询目的：统计成功+失败 placement 的 request 级 placement_cnt 分布，并新增 country 维度。
-- 输出字段：experiment_group, product, ad_format, country, placement_cnt_bucket, request_pv, denominator_request_pv, total_request_pv, share

WITH per_request_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    COALESCE(country, 'UNKNOWN') AS country,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT IF(status IN ('AD_LOADED', 'FAILED_TO_LOAD'), placement_id, NULL)) AS placement_cnt
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
  GROUP BY experiment_group, product, ad_format, country, user_pseudo_id, request_id
)

SELECT
  p.experiment_group,
  p.product,
  p.ad_format,
  p.country,
  CAST(p.placement_cnt AS STRING) AS placement_cnt_bucket,
  COUNT(*) AS request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY p.experiment_group, p.product, p.ad_format, p.country
  ) AS denominator_request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY p.experiment_group, p.product, p.ad_format, p.country
  ) AS total_request_pv,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (
      PARTITION BY p.experiment_group, p.product, p.ad_format, p.country
    )
  ) AS share
FROM per_request_counts p
WHERE p.placement_cnt > 0
GROUP BY p.experiment_group, p.product, p.ad_format, p.country, placement_cnt_bucket
ORDER BY
  p.experiment_group,
  p.product,
  p.ad_format,
  p.country,
  SAFE_CAST(placement_cnt_bucket AS INT64);
