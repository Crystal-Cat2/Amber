-- 查询目的：找出 B 组中 max_request_ratio 与 latency_ratio 差值最大的用户，便于后续下钻。
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
    r.request_id,
    MIN(r.event_timestamp) AS request_ts
  FROM app_base r
  JOIN group_ranges g
    ON r.user_pseudo_id = g.user_pseudo_id
   AND r.event_timestamp >= g.group_start_ts
   AND (r.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE r.event_name = 'adslog_request'
    AND r.request_id IS NOT NULL
    AND g.experiment_group = 'B'
  GROUP BY r.user_pseudo_id, g.experiment_group, r.request_id
),
max_request_base AS (
  SELECT
    r.user_pseudo_id,
    g.experiment_group,
    r.max_request_id
  FROM app_base r
  JOIN group_ranges g
    ON r.user_pseudo_id = g.user_pseudo_id
   AND r.event_timestamp >= g.group_start_ts
   AND (r.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE r.event_name = 'lib_isx_max_request'
    AND r.max_request_id IS NOT NULL
    AND g.experiment_group = 'B'
  GROUP BY r.user_pseudo_id, g.experiment_group, r.max_request_id
),
latency_base AS (
  SELECT
    l.user_pseudo_id,
    g.experiment_group,
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
    AND g.experiment_group = 'B'
  GROUP BY l.user_pseudo_id, g.experiment_group, l.request_id, l.network, l.status
),
latency_total AS (
  SELECT user_pseudo_id, request_id
  FROM latency_base
  GROUP BY user_pseudo_id, request_id
),
latency_qualified AS (
  SELECT user_pseudo_id, request_id
  FROM latency_base
  WHERE LOWER(COALESCE(network, '')) = 'isadxcustomadapter'
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD')
  GROUP BY user_pseudo_id, request_id
),
adslog_agg AS (
  SELECT user_pseudo_id, COUNT(DISTINCT request_id) AS adslog_request_cnt, MIN(request_ts) AS first_request_ts, MAX(request_ts) AS last_request_ts
  FROM adslog_request_base
  GROUP BY user_pseudo_id
),
max_agg AS (
  SELECT user_pseudo_id, COUNT(DISTINCT max_request_id) AS lib_isx_max_request_cnt
  FROM max_request_base
  GROUP BY user_pseudo_id
),
latency_total_agg AS (
  SELECT user_pseudo_id, COUNT(DISTINCT request_id) AS total_latency_request_cnt
  FROM latency_total
  GROUP BY user_pseudo_id
),
latency_qualified_agg AS (
  SELECT user_pseudo_id, COUNT(DISTINCT request_id) AS qualified_latency_request_cnt
  FROM latency_qualified
  GROUP BY user_pseudo_id
)
SELECT
  a.user_pseudo_id,
  'B' AS experiment_group,
  'screw_puzzle' AS product,
  a.adslog_request_cnt,
  COALESCE(m.lib_isx_max_request_cnt, 0) AS lib_isx_max_request_cnt,
  COALESCE(lt.total_latency_request_cnt, 0) AS total_latency_request_cnt,
  COALESCE(lq.qualified_latency_request_cnt, 0) AS qualified_latency_request_cnt,
  SAFE_DIVIDE(COALESCE(m.lib_isx_max_request_cnt, 0), NULLIF(a.adslog_request_cnt, 0)) AS max_request_ratio,
  SAFE_DIVIDE(COALESCE(lq.qualified_latency_request_cnt, 0), NULLIF(COALESCE(lt.total_latency_request_cnt, 0), 0)) AS latency_ratio,
  SAFE_DIVIDE(COALESCE(lq.qualified_latency_request_cnt, 0), NULLIF(COALESCE(lt.total_latency_request_cnt, 0), 0))
    - SAFE_DIVIDE(COALESCE(m.lib_isx_max_request_cnt, 0), NULLIF(a.adslog_request_cnt, 0)) AS ratio_gap,
  a.first_request_ts,
  a.last_request_ts
FROM adslog_agg a
LEFT JOIN max_agg m
  ON a.user_pseudo_id = m.user_pseudo_id
LEFT JOIN latency_total_agg lt
  ON a.user_pseudo_id = lt.user_pseudo_id
LEFT JOIN latency_qualified_agg lq
  ON a.user_pseudo_id = lq.user_pseudo_id
WHERE a.adslog_request_cnt > 0
  AND COALESCE(lt.total_latency_request_cnt, 0) > 0
ORDER BY ABS(ratio_gap) DESC,
         ABS(COALESCE(lq.qualified_latency_request_cnt, 0) - COALESCE(m.lib_isx_max_request_cnt, 0)) DESC,
         a.adslog_request_cnt DESC,
         a.last_request_ts DESC
LIMIT 1;
