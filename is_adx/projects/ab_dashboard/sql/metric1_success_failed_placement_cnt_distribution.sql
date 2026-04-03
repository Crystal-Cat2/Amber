-- 查询目的：统计全量 / 有成功 / 无成功 request 的成功+失败 placement 的 request 级 placement_cnt 分布。
-- 输出字段：experiment_group, product, ad_format, success_scope, placement_cnt_bucket, request_pv, denominator_request_pv, total_request_pv, share
-- 关键口径：
-- 1. 先按当前 placement 结构页的基础过滤定义 request，结果按 experiment_group + product + ad_format 分组展示。
-- 2. 分子只统计 status 为 AD_LOADED 或 FAILED_TO_LOAD 的 placement，并按 placement_id 去重后统计个数。
-- 3. request 级成功定义：同一 request 内存在任一 status = 'AD_LOADED'。
-- 4. 每个 placement 只按唯一 placement_id 计 1 次；placement_cnt >= 22 合并为 22+。

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    placement_id,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
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
    COUNT(DISTINCT IF(status IN ('AD_LOADED', 'FAILED_TO_LOAD'), placement_id, NULL)) AS placement_cnt
  FROM request_success_scopes s
  INNER JOIN base_latency b
    ON s.experiment_group = b.experiment_group
    AND s.product = b.product
    AND s.ad_format = b.ad_format
    AND s.user_pseudo_id = b.user_pseudo_id
    AND s.request_id = b.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
)

SELECT
  p.experiment_group,
  p.product,
  p.ad_format,
  p.success_scope,
  CASE
    WHEN p.placement_cnt >= 22 THEN '22+'
    ELSE CAST(p.placement_cnt AS STRING)
  END AS placement_cnt_bucket,
  COUNT(*) AS request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY p.experiment_group, p.product, p.ad_format, p.success_scope
  ) AS denominator_request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY p.experiment_group, p.product, p.ad_format, p.success_scope
  ) AS total_request_pv,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (
      PARTITION BY p.experiment_group, p.product, p.ad_format, p.success_scope
    )
  ) AS share
FROM per_request_counts p
WHERE p.placement_cnt > 0
GROUP BY p.experiment_group, p.product, p.ad_format, p.success_scope, placement_cnt_bucket
ORDER BY
  p.product,
  p.ad_format,
  p.success_scope,
  p.experiment_group,
  CASE
    WHEN placement_cnt_bucket = '22+' THEN 22
    ELSE SAFE_CAST(placement_cnt_bucket AS INT64)
  END;
