-- 查询目的：固定 total network_cnt、network_type 与 type_network_cnt 后，统计全量 / 有成功 / 无成功 request 的三种 status 分布。
-- 输出字段：experiment_group, product, ad_format, success_scope, network_cnt, network_type, type_network_cnt, status_bucket, request_pv, denominator_request_pv, share
-- 关键口径：
-- 1. 统计对象是全量去重 request，不再按请求顺序编号，也不限制前 200 次。
-- 2. 总 network_cnt 与 type_network_cnt 都按去重后的 type + network 计算；只统计 bidding / waterfall，不再额外过滤 request。
-- 3. bidding：直接按原始三种状态统计，同一 request 可同时落入多个 status。
-- 4. waterfall：同一 request 若同时命中多个状态，按 AD_LOADED > FAILED_TO_LOAD > AD_LOAD_NOT_ATTEMPTED 归并为唯一 status。
-- 5. request 级成功定义：同一 request 内存在任一 status = 'AD_LOADED'。
-- 6. share = 当前 total_cnt + network_type + type_network_cnt + success_scope 桶内，落到该 status 的 request_pv / 该桶总 request_pv。

WITH base_latency AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network,
    LOWER(COALESCE(network_type, '')) AS network_type,
    status
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
    network,
    network_type
  FROM base_latency
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
    COUNT(DISTINCT TO_JSON_STRING(STRUCT(r.network_type, r.network))) AS network_cnt,
    COUNT(DISTINCT IF(r.network_type = 'bidding', TO_JSON_STRING(STRUCT(r.network_type, r.network)), NULL)) AS bidding_cnt,
    COUNT(DISTINCT IF(r.network_type = 'waterfall', TO_JSON_STRING(STRUCT(r.network_type, r.network)), NULL)) AS waterfall_cnt
  FROM request_success_scopes s
  INNER JOIN typed_request_network r
    ON s.experiment_group = r.experiment_group
    AND s.product = r.product
    AND s.ad_format = r.ad_format
    AND s.user_pseudo_id = r.user_pseudo_id
    AND s.request_id = r.request_id
  GROUP BY s.experiment_group, s.product, s.ad_format, s.success_scope, s.user_pseudo_id, s.request_id
),

type_request_counts AS (
  SELECT
    p.experiment_group,
    p.product,
    p.ad_format,
    p.success_scope,
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
    AND p.user_pseudo_id = r.user_pseudo_id
    AND p.request_id = r.request_id
  GROUP BY p.experiment_group, p.product, p.ad_format, p.success_scope, p.user_pseudo_id, p.request_id, p.network_cnt, r.network_type
),

request_status_detail AS (
  SELECT DISTINCT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    network_type,
    COALESCE(status, 'NULL_STATUS') AS status
  FROM base_latency
),

bidding_status_requests AS (
  SELECT DISTINCT
    t.experiment_group,
    t.product,
    t.ad_format,
    t.success_scope,
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
    t.success_scope,
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
    AND t.user_pseudo_id = d.user_pseudo_id
    AND t.request_id = d.request_id
  WHERE t.network_type = 'waterfall'
    AND d.network_type = 'waterfall'
    AND d.status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
  GROUP BY
    t.experiment_group,
    t.product,
    t.ad_format,
    t.success_scope,
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
    success_scope,
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
    success_scope,
    network_cnt,
    network_type,
    type_network_cnt,
    COUNT(*) AS denominator_request_pv
  FROM type_request_counts
  GROUP BY experiment_group, product, ad_format, success_scope, network_cnt, network_type, type_network_cnt
),

status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    success_scope,
    network_cnt,
    network_type,
    type_network_cnt,
    status_bucket,
    COUNT(*) AS request_pv
  FROM request_status_bucket
  WHERE status_bucket IS NOT NULL
  GROUP BY experiment_group, product, ad_format, success_scope, network_cnt, network_type, type_network_cnt, status_bucket
)

SELECT
  c.experiment_group,
  c.product,
  c.ad_format,
  c.success_scope,
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
  AND c.success_scope = d.success_scope
  AND c.network_cnt = d.network_cnt
  AND c.network_type = d.network_type
  AND c.type_network_cnt = d.type_network_cnt
ORDER BY
  c.product,
  c.ad_format,
  c.success_scope,
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
