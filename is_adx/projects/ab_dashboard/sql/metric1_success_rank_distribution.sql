-- 查询目的：统计成功 request 在不同 network_cnt / placement_cnt 桶内的 success_rank 分布。
-- 输出字段：experiment_group, product, ad_format, success_scope, cnt_type, cnt_value, success_rank, request_pv, bucket_success_request_pv, bucket_total_request_pv
-- 关键口径：
-- 1. cnt 桶沿用 metric1 旧口径：network_cnt = 去重后的 type + network 个数；placement_cnt = placement 行数，不去重。
-- 2. success_rank 只在成功 request 内计算，定义为失败次数 + 首次成功序号；当前 SQL 用 fail_cnt + 1 表示。
-- 3. 图内分母使用当前 cnt 桶内成功 request_pv；右上角摘要额外给出当前 cnt 桶总 request_pv 与成功率。

WITH base_rows AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    LOWER(COALESCE(network_type, '')) AS network_type,
    network,
    placement_id,
    status,
    event_timestamp
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
    AND experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
),

dedup_rows AS (
  SELECT * EXCEPT(status_row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id, status, network_type, network, placement_id
        ORDER BY event_timestamp ASC
      ) AS status_row_num
    FROM base_rows
  )
  WHERE status_row_num = 1
),

request_success AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_ad_loaded
  FROM dedup_rows
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

success_scope_rows AS (
  SELECT experiment_group, product, ad_format, user_pseudo_id, request_id, 'all' AS success_scope
  FROM request_success
  WHERE has_ad_loaded = 1
  UNION ALL
  SELECT experiment_group, product, ad_format, user_pseudo_id, request_id, 'has_success' AS success_scope
  FROM request_success
  WHERE has_ad_loaded = 1
),

network_bucket_base AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT CONCAT(network_type, '||', network)) AS network_cnt
  FROM dedup_rows
  WHERE status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

placement_bucket_base AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(*) AS placement_cnt
  FROM dedup_rows
  WHERE placement_id IS NOT NULL
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

network_total_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    network_cnt,
    COUNT(*) AS bucket_total_request_pv
  FROM network_bucket_base
  GROUP BY experiment_group, product, ad_format, network_cnt
),

placement_total_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    placement_cnt,
    COUNT(*) AS bucket_total_request_pv
  FROM placement_bucket_base
  GROUP BY experiment_group, product, ad_format, placement_cnt
),

fail_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(*) AS fail_cnt
  FROM dedup_rows
  WHERE status = 'FAILED_TO_LOAD'
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

success_rank_rows AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    event_timestamp
  FROM dedup_rows
  WHERE status = 'AD_LOADED'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id
    ORDER BY event_timestamp ASC
  ) = 1
),

success_request_base AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    scope.success_scope,
    s.user_pseudo_id,
    s.request_id,
    COALESCE(f.fail_cnt, 0) + 1 AS success_rank
  FROM success_rank_rows s
  INNER JOIN success_scope_rows scope
    ON s.experiment_group = scope.experiment_group
    AND s.product = scope.product
    AND s.ad_format = scope.ad_format
    AND s.user_pseudo_id = scope.user_pseudo_id
    AND s.request_id = scope.request_id
  LEFT JOIN fail_counts f
    ON s.experiment_group = f.experiment_group
    AND s.product = f.product
    AND s.ad_format = f.ad_format
    AND s.user_pseudo_id = f.user_pseudo_id
    AND s.request_id = f.request_id
),

network_success_fact AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    s.success_scope,
    n.network_cnt,
    s.success_rank
  FROM success_request_base s
  INNER JOIN network_bucket_base n
    ON s.experiment_group = n.experiment_group
    AND s.product = n.product
    AND s.ad_format = n.ad_format
    AND s.user_pseudo_id = n.user_pseudo_id
    AND s.request_id = n.request_id
),

network_success_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    network_cnt,
    COUNT(*) AS bucket_success_request_pv
  FROM network_success_fact
  GROUP BY experiment_group, product, ad_format, success_scope, network_cnt
),

placement_success_fact AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    s.success_scope,
    p.placement_cnt,
    s.success_rank
  FROM success_request_base s
  INNER JOIN placement_bucket_base p
    ON s.experiment_group = p.experiment_group
    AND s.product = p.product
    AND s.ad_format = p.ad_format
    AND s.user_pseudo_id = p.user_pseudo_id
    AND s.request_id = p.request_id
),

placement_success_bucket AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    placement_cnt,
    COUNT(*) AS bucket_success_request_pv
  FROM placement_success_fact
  GROUP BY experiment_group, product, ad_format, success_scope, placement_cnt
),

network_rank_distribution AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
    'network' AS cnt_type,
    CAST(p.network_cnt AS STRING) AS cnt_value,
    CAST(p.success_rank AS STRING) AS success_rank,
    COUNT(*) AS request_pv,
    b.bucket_success_request_pv,
    t.bucket_total_request_pv
  FROM network_success_fact p
  INNER JOIN network_success_bucket b
    ON p.experiment_group = b.experiment_group
    AND p.product = b.product
    AND p.ad_format = b.ad_format
    AND p.success_scope = b.success_scope
    AND p.network_cnt = b.network_cnt
  INNER JOIN network_total_bucket t
    ON p.experiment_group = t.experiment_group
    AND p.product = t.product
    AND p.ad_format = t.ad_format
    AND p.network_cnt = t.network_cnt
  GROUP BY
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
    cnt_type,
    cnt_value,
    success_rank,
    b.bucket_success_request_pv,
    t.bucket_total_request_pv
),

placement_rank_distribution AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
    'placement' AS cnt_type,
    CAST(p.placement_cnt AS STRING) AS cnt_value,
    CAST(p.success_rank AS STRING) AS success_rank,
    COUNT(*) AS request_pv,
    b.bucket_success_request_pv,
    t.bucket_total_request_pv
  FROM placement_success_fact p
  INNER JOIN placement_success_bucket b
    ON p.experiment_group = b.experiment_group
    AND p.product = b.product
    AND p.ad_format = b.ad_format
    AND p.success_scope = b.success_scope
    AND p.placement_cnt = b.placement_cnt
  INNER JOIN placement_total_bucket t
    ON p.experiment_group = t.experiment_group
    AND p.product = t.product
    AND p.ad_format = t.ad_format
    AND p.placement_cnt = t.placement_cnt
  GROUP BY
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
    cnt_type,
    cnt_value,
    success_rank,
    b.bucket_success_request_pv,
    t.bucket_total_request_pv
)

SELECT *
FROM network_rank_distribution

UNION ALL

SELECT *
FROM placement_rank_distribution

ORDER BY
  product,
  ad_format,
  success_scope,
  experiment_group,
  cnt_type,
  SAFE_CAST(cnt_value AS INT64),
  SAFE_CAST(success_rank AS INT64);
