-- 查询目的：统计 B 组 request 未匹配 latency 且伴随 adslog_error 的 err_msg 分布。
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
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='group') AS ab_group,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('err_msg', 'error_massage')),
      ''
    ) AS err_msg,
    CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='err_type') AS STRING) AS err_type,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='network_name'),
      ''
    ) AS network_name
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
),
adslog_error_detail AS (
  SELECT
    e.user_pseudo_id,
    e.request_id,
    NULLIF(e.err_msg, '') AS err_msg,
    NULLIF(e.err_type, '') AS err_type,
    NULLIF(e.network_name, '') AS network_name
  FROM app_base e
  JOIN group_ranges g
    ON e.user_pseudo_id = g.user_pseudo_id
   AND e.event_timestamp >= g.group_start_ts
   AND (e.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE e.event_name = 'adslog_error'
    AND g.ab_group = 'B'
    AND e.request_id IS NOT NULL
)
SELECT
  r.ad_format,
  COALESCE(e.err_msg, '__NO_ERR_MSG__') AS err_msg,
  COALESCE(e.err_type, '__NO_ERR_TYPE__') AS err_type,
  COALESCE(e.network_name, '__NO_NETWORK__') AS network_name,
  COUNT(DISTINCT CONCAT(r.user_pseudo_id, '#', r.request_id)) AS unmatched_request_cnt
FROM request_without_latency r
LEFT JOIN adslog_error_detail e
  ON r.user_pseudo_id = e.user_pseudo_id
 AND r.request_id = e.request_id
GROUP BY r.ad_format, err_msg, err_type, network_name
ORDER BY unmatched_request_cnt DESC, r.ad_format, err_msg;
