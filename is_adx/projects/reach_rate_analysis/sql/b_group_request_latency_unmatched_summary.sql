-- 查询目的：统计 B 组 adslog_request 中未匹配到 latency 的 request，并区分是否伴随 adslog_error。
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
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='group') AS ab_group
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('adslog_request', 'adslog_error', 'lib_isx_group')
),
group_ranges AS (
  SELECT
    user_pseudo_id,
    ab_group,
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
    r.request_id,
    r.ad_format
  FROM app_base r
  JOIN group_ranges g
    ON r.user_pseudo_id = g.user_pseudo_id
   AND r.event_timestamp >= g.group_start_ts
   AND (r.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE r.event_name = 'adslog_request'
    AND g.ab_group = 'B'
    AND r.request_id IS NOT NULL
  GROUP BY r.user_pseudo_id, r.request_id, r.ad_format
),
latency_request_base AS (
  SELECT
    user_pseudo_id,
    request_id
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE product = 'com.takeoffbolts.screw.puzzle'
    AND experiment_group = 'have_is_adx'
    AND request_id IS NOT NULL
    AND DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
  GROUP BY user_pseudo_id, request_id
),
adslog_error_base AS (
  SELECT
    e.user_pseudo_id,
    e.request_id
  FROM app_base e
  JOIN group_ranges g
    ON e.user_pseudo_id = g.user_pseudo_id
   AND e.event_timestamp >= g.group_start_ts
   AND (e.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE e.event_name = 'adslog_error'
    AND g.ab_group = 'B'
    AND e.request_id IS NOT NULL
  GROUP BY e.user_pseudo_id, e.request_id
),
request_without_latency AS (
  SELECT
    r.user_pseudo_id,
    r.request_id,
    r.ad_format
  FROM adslog_request_base r
  LEFT JOIN latency_request_base l
    ON r.user_pseudo_id = l.user_pseudo_id
   AND r.request_id = l.request_id
  WHERE l.request_id IS NULL
)
SELECT
  ad_format,
  COUNT(*) AS unmatched_request_cnt,
  COUNTIF(e.request_id IS NOT NULL) AS unmatched_with_adslog_error_cnt,
  COUNTIF(e.request_id IS NULL) AS unmatched_without_adslog_error_cnt,
  SAFE_DIVIDE(COUNTIF(e.request_id IS NOT NULL), COUNT(*)) AS unmatched_with_adslog_error_ratio
FROM request_without_latency r
LEFT JOIN adslog_error_base e
  ON r.user_pseudo_id = e.user_pseudo_id
 AND r.request_id = e.request_id
GROUP BY ad_format
ORDER BY unmatched_request_cnt DESC;
