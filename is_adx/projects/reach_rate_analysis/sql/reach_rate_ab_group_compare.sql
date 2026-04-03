-- 查询目的：对比 A/B 组在 adslog_request、lib_isx_max_request 与 latency 覆盖上的请求级差异。
-- 关键口径：
-- 1. latency_ratio = IsAdxCustomAdapter 渠道中成功/失败状态的去重 latency 请求数 / 全量 latency 去重 request_id 数
-- 2. max_request_ratio = lib_isx_max_request 去重 max_request_id 数 / adslog_request 去重 request_id 数
WITH app_base AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      WHEN event_name LIKE 'banner%' THEN 'banner'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='max_request_id') AS max_request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='group') AS ab_group
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_request', 'lib_isx_max_request', 'lib_isx_group')
),
group_ranges AS (
  SELECT
    user_pseudo_id,
    ab_group AS experiment_group,
    event_timestamp AS group_start_ts,
    LEAD(event_timestamp) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY event_timestamp
    ) AS group_end_ts
  FROM app_base
  WHERE event_name = 'lib_isx_group'
    AND ab_group IN ('A', 'B')
),
adslog_request_base AS (
  SELECT
    r.user_pseudo_id,
    g.experiment_group,
    'screw_puzzle' AS product,
    r.ad_format,
    r.request_id,
    MIN(r.event_timestamp) AS request_ts
  FROM app_base r
  JOIN group_ranges g
    ON r.user_pseudo_id = g.user_pseudo_id
   AND r.event_timestamp >= g.group_start_ts
   AND (r.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE r.event_name = 'adslog_request'
    AND r.request_id IS NOT NULL
  GROUP BY r.user_pseudo_id, g.experiment_group, product, r.ad_format, r.request_id
),
max_request_base AS (
  SELECT
    r.user_pseudo_id,
    g.experiment_group,
    r.ad_format,
    r.max_request_id
  FROM app_base r
  JOIN group_ranges g
    ON r.user_pseudo_id = g.user_pseudo_id
   AND r.event_timestamp >= g.group_start_ts
   AND (r.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE r.event_name = 'lib_isx_max_request'
    AND r.max_request_id IS NOT NULL
  GROUP BY r.user_pseudo_id, g.experiment_group, r.ad_format, r.max_request_id
),
latency_base AS (
  SELECT
    l.user_pseudo_id,
    g.experiment_group,
    LOWER(COALESCE(l.ad_format, 'unknown')) AS ad_format,
    l.request_id,
    l.network,
    l.status,
    MIN(l.event_timestamp) AS latency_ts
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail` l
  JOIN group_ranges g
    ON l.user_pseudo_id = g.user_pseudo_id
   AND l.event_timestamp >= g.group_start_ts
   AND (l.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE l.product = 'com.takeoffbolts.screw.puzzle'
    AND DATE(TIMESTAMP_MICROS(l.event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
    AND l.request_id IS NOT NULL
  GROUP BY l.user_pseudo_id, g.experiment_group, ad_format, l.request_id, l.network, l.status
),
latency_total AS (
  SELECT
    experiment_group,
    ad_format,
    user_pseudo_id,
    request_id
  FROM latency_base
  GROUP BY experiment_group, ad_format, user_pseudo_id, request_id
),
latency_qualified AS (
  SELECT
    experiment_group,
    ad_format,
    user_pseudo_id,
    request_id
  FROM latency_base
  WHERE LOWER(COALESCE(network, '')) = 'isadxcustomadapter'
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD')
  GROUP BY experiment_group, ad_format, user_pseudo_id, request_id
),
adslog_agg AS (
  SELECT experiment_group, product, ad_format, COUNT(DISTINCT request_id) AS adslog_request_cnt
  FROM adslog_request_base
  GROUP BY experiment_group, product, ad_format
),
max_agg AS (
  SELECT experiment_group, ad_format, COUNT(DISTINCT max_request_id) AS lib_isx_max_request_cnt
  FROM max_request_base
  GROUP BY experiment_group, ad_format
),
latency_total_agg AS (
  SELECT experiment_group, ad_format, COUNT(DISTINCT request_id) AS total_latency_request_cnt
  FROM latency_total
  GROUP BY experiment_group, ad_format
),
latency_qualified_agg AS (
  SELECT experiment_group, ad_format, COUNT(DISTINCT request_id) AS qualified_latency_request_cnt
  FROM latency_qualified
  GROUP BY experiment_group, ad_format
),
keys AS (
  SELECT experiment_group, ad_format FROM adslog_agg
  UNION DISTINCT
  SELECT experiment_group, ad_format FROM max_agg
  UNION DISTINCT
  SELECT experiment_group, ad_format FROM latency_total_agg
  UNION DISTINCT
  SELECT experiment_group, ad_format FROM latency_qualified_agg
)
SELECT
  k.experiment_group,
  'screw_puzzle' AS product,
  k.ad_format,
  COALESCE(a.adslog_request_cnt, 0) AS adslog_request_cnt,
  COALESCE(m.lib_isx_max_request_cnt, 0) AS lib_isx_max_request_cnt,
  COALESCE(lt.total_latency_request_cnt, 0) AS total_latency_request_cnt,
  COALESCE(lq.qualified_latency_request_cnt, 0) AS qualified_latency_request_cnt,
  SAFE_DIVIDE(COALESCE(m.lib_isx_max_request_cnt, 0), NULLIF(COALESCE(a.adslog_request_cnt, 0), 0)) AS max_request_ratio,
  SAFE_DIVIDE(COALESCE(lq.qualified_latency_request_cnt, 0), NULLIF(COALESCE(lt.total_latency_request_cnt, 0), 0)) AS latency_ratio,
  SAFE_DIVIDE(COALESCE(lq.qualified_latency_request_cnt, 0), NULLIF(COALESCE(lt.total_latency_request_cnt, 0), 0))
    - SAFE_DIVIDE(COALESCE(m.lib_isx_max_request_cnt, 0), NULLIF(COALESCE(a.adslog_request_cnt, 0), 0)) AS ratio_gap
FROM keys k
LEFT JOIN adslog_agg a
  ON k.experiment_group = a.experiment_group
 AND k.ad_format = a.ad_format
LEFT JOIN max_agg m
  ON k.experiment_group = m.experiment_group
 AND k.ad_format = m.ad_format
LEFT JOIN latency_total_agg lt
  ON k.experiment_group = lt.experiment_group
 AND k.ad_format = lt.ad_format
LEFT JOIN latency_qualified_agg lq
  ON k.experiment_group = lq.experiment_group
 AND k.ad_format = lq.ad_format
ORDER BY k.experiment_group, ABS(ratio_gap) DESC, k.ad_format;
