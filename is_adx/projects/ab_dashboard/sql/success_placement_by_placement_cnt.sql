-- 查询目的：先按 request 的全量 placement_cnt 分桶，再统计桶内最终成功 placement 的占比。
-- 输出字段：experiment_group, product, ad_format, placement_cnt, success_target, request_pv, denominator_request_pv, share
-- 关键口径：
-- 1. placement_cnt 按单次 request 内的 placement 行数统计，不去重 placement。
-- 2. 成功只认 AD_LOADED；若当前 request 没有任何成功，则 success_target 记为 fail。
-- 3. share = 当前 placement_cnt 桶内，success_target 的 request_pv / 当前 placement_cnt 桶总 request_pv。

WITH request_placement_detail AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    placement_id
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

per_request_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(*) AS placement_cnt
  FROM request_placement_detail
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

success_rows AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    placement_id AS success_target,
    ROW_NUMBER() OVER (
      PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id
      ORDER BY event_timestamp DESC, placement_id DESC
    ) AS success_rank
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
    AND status = 'AD_LOADED'
),

request_targets AS (
  SELECT
    c.experiment_group,
    c.product,
    c.ad_format,
    c.user_pseudo_id,
    c.request_id,
    c.placement_cnt,
    COALESCE(s.success_target, 'fail') AS success_target
  FROM per_request_counts c
  LEFT JOIN success_rows s
    ON c.experiment_group = s.experiment_group
    AND c.product = s.product
    AND c.ad_format = s.ad_format
    AND c.user_pseudo_id = s.user_pseudo_id
    AND c.request_id = s.request_id
    AND s.success_rank = 1
),

denominator_totals AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    placement_cnt,
    COUNT(*) AS denominator_request_pv
  FROM request_targets
  GROUP BY experiment_group, product, ad_format, placement_cnt
),

bucket_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    placement_cnt,
    success_target,
    COUNT(*) AS request_pv
  FROM request_targets
  GROUP BY experiment_group, product, ad_format, placement_cnt, success_target
)

SELECT
  b.experiment_group,
  b.product,
  b.ad_format,
  b.placement_cnt,
  b.success_target,
  b.request_pv,
  d.denominator_request_pv,
  SAFE_DIVIDE(b.request_pv, d.denominator_request_pv) AS share
FROM bucket_counts b
INNER JOIN denominator_totals d
  ON b.experiment_group = d.experiment_group
  AND b.product = d.product
  AND b.ad_format = d.ad_format
  AND b.placement_cnt = d.placement_cnt
ORDER BY
  b.product,
  b.ad_format,
  b.experiment_group,
  b.placement_cnt,
  b.request_pv DESC,
  b.success_target;
