-- 查询目的：查看目标 B 组用户在广告请求链路中的关键事件时间线，便于定位 latency/max_request 缺失位置。
DECLARE target_user STRING DEFAULT '{target_user}';

WITH app_base AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    event_name,
    geo.country AS country,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', TIMESTAMP_MICROS(event_timestamp), 'UTC') AS utc_datetime,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      WHEN event_name LIKE 'banner%' THEN 'banner'
      ELSE 'unknown'
    END AS ad_format,
    (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='ga_session_number') AS ga_session_number,
    (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='rid') AS level_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='from') AS entrance,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='can_reive') AS can_reive,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='wasRevive') AS was_revive,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('max_unit_id','unit_id','sdk_unit_id')) AS max_unit_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='request_scene') AS request_scene,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('Scene','impression_scene')) AS scene,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='cache') AS cache,
    (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='step') AS step,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='request_type') AS request_type,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='network_name') AS network_name,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='network_placement') AS network_placement,
    ROUND(
      (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_time','duration','ad_storage_time')) / 1000.0,
      2
    ) AS duration,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='isx_request_id') AS isx_request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='isx_unit_id') AS isx_unit_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='scene') AS isx_request_scene,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='isx_network') AS isx_network,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='max_request_id') AS max_request_id,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='max_placement_id') AS max_placement_id,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key='filled_value') AS filled_value,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key='floor_price') AS floor_price,
    (SELECT value.double_value FROM UNNEST(event_params.array) WHERE key='max_price') AS max_price,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='lib_net_status') AS lib_net_status,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('err_msg','error_massage')) AS err_msg,
    (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='err_type') AS err_type,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('conf_id','config_id')) AS conf_id,
    (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key='load_method') AS load_method,
    (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key='group') AS ab_group
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND user_pseudo_id = target_user
    AND event_name IN (
      'first_open','app_open','lib_mediation_initialize',
      'adslog_request','adslog_filled','adslog_error','adslog_imp',
      'interstitial_ad_request','interstitial_ad_fill','interstitial_ad_failed',
      'reward_ad_request','reward_ad_fill','reward_ad_failed',
      'interstitial_ad_show','reward_ad_show',
      'interstitial_ad_impression','reward_ad_impression',
      'interstitial_ad_display_failed','reward_ad_display_faile',
      'interstitial_ad_trigger','reward_ad_trigger',
      'lib_isx_group','lib_isx_request','lib_isx_fill','lib_isx_max_request','lib_isx_max_fill',
      'lib_isx_imp','lib_isx_click','lib_isx_error','lib_isx_close',
      'lib_tpx_request','lib_tpx_fill','lib_tpx_max_request','lib_tpx_max_fill',
      'lib_tpx_imp','lib_tpx_click','lib_tpx_error','lib_tpx_close',
      'classic_new_game_start','classic_finish_revive_back','classic_game_over',
      'classic_game_complete_over','classic_revive_show','classic_revive_timeout','classic_revive_click',
      'app_quit','app_resume','lassic_revive_close_click'
    )
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
app_events_b AS (
  SELECT
    b.*
  FROM app_base b
  JOIN group_ranges g
    ON b.user_pseudo_id = g.user_pseudo_id
   AND b.event_timestamp >= g.group_start_ts
   AND (b.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE g.ab_group = 'B'
),
app_events_with_prev AS (
  SELECT
    *,
    LAG(event_timestamp) OVER (
      PARTITION BY user_pseudo_id, ga_session_number, event_name
      ORDER BY event_timestamp
    ) AS prev_request_ts,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, request_id
      ORDER BY event_timestamp
    ) AS request_row_num,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, max_request_id
      ORDER BY event_timestamp
    ) AS max_request_row_num
  FROM app_events_b
),
latency_base AS (
  SELECT
    l.user_pseudo_id,
    LOWER(COALESCE(l.ad_format, 'unknown')) AS ad_format,
    l.request_id,
    l.network,
    l.status,
    l.event_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY l.user_pseudo_id, l.request_id
      ORDER BY l.event_timestamp
    ) AS latency_request_row_num,
    COUNT(*) OVER (
      PARTITION BY l.user_pseudo_id, l.request_id
    ) AS latency_event_cnt
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail` l
  JOIN group_ranges g
    ON l.user_pseudo_id = g.user_pseudo_id
   AND l.event_timestamp >= g.group_start_ts
   AND (l.event_timestamp < g.group_end_ts OR g.group_end_ts IS NULL)
  WHERE l.product = 'com.takeoffbolts.screw.puzzle'
    AND DATE(TIMESTAMP_MICROS(l.event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
    AND l.user_pseudo_id = target_user
    AND l.request_id IS NOT NULL
    AND g.ab_group = 'B'
),
latency_total_events AS (
  SELECT
    user_pseudo_id,
    ad_format,
    request_id,
    MIN(event_timestamp) AS latency_ts,
    MAX(latency_event_cnt) AS latency_event_cnt
  FROM latency_base
  GROUP BY user_pseudo_id, ad_format, request_id
),
latency_qualified_events AS (
  SELECT
    user_pseudo_id,
    ad_format,
    request_id,
    MIN(event_timestamp) AS qualified_latency_ts,
    COUNT(*) AS qualified_latency_event_cnt,
    STRING_AGG(DISTINCT status, ',' ORDER BY status) AS qualified_latency_status_set
  FROM latency_base
  WHERE LOWER(COALESCE(network, '')) = 'isadxcustomadapter'
    AND status IN ('AD_LOADED', 'FAILED_TO_LOAD')
  GROUP BY user_pseudo_id, ad_format, request_id
),
merged AS (
  SELECT
    user_pseudo_id,
    'B' AS experiment_group,
    'screw_puzzle' AS product,
    country,
    event_timestamp,
    event_name,
    CONCAT(utc_datetime, ' UTC') AS utc_datetime,
    conf_id,
    load_method,
    scene,
    ROUND(
      CASE
        WHEN event_name LIKE '%request%' AND prev_request_ts IS NOT NULL
          THEN (event_timestamp - prev_request_ts) / 1000000.0
        ELSE NULL
      END,
      4
    ) AS diff_prev_request_sec,
    ad_format,
    duration,
    cache,
    request_type,
    request_id,
    max_unit_id,
    request_scene,
    step,
    filled_value,
    network_name,
    network_placement,
    isx_request_id,
    isx_unit_id,
    isx_request_scene,
    isx_network,
    max_request_id,
    max_placement_id,
    floor_price,
    max_price,
    lib_net_status,
    err_msg,
    err_type,
    ga_session_number,
    level_id,
    entrance,
    can_reive,
    was_revive,
    IF(event_name = 'adslog_request' AND request_id IS NOT NULL AND request_row_num = 1, 1, 0) AS adslog_request_step_cnt,
    IF(event_name = 'lib_isx_max_request' AND max_request_id IS NOT NULL AND max_request_row_num = 1, 1, 0) AS lib_isx_max_request_step_cnt,
    0 AS total_latency_step_cnt,
    0 AS qualified_latency_step_cnt,
    0 AS latency_event_cnt,
    0 AS qualified_latency_event_cnt,
    NULL AS qualified_latency_status_set
  FROM app_events_with_prev

  UNION ALL

  SELECT
    user_pseudo_id,
    'B' AS experiment_group,
    'screw_puzzle' AS product,
    NULL AS country,
    latency_ts AS event_timestamp,
    'latency_total' AS event_name,
    CONCAT(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', TIMESTAMP_MICROS(latency_ts), 'UTC'), ' UTC') AS utc_datetime,
    NULL AS conf_id,
    NULL AS load_method,
    NULL AS scene,
    NULL AS diff_prev_request_sec,
    ad_format,
    NULL AS duration,
    NULL AS cache,
    NULL AS request_type,
    request_id,
    NULL AS max_unit_id,
    NULL AS request_scene,
    NULL AS step,
    NULL AS filled_value,
    NULL AS network_name,
    NULL AS network_placement,
    NULL AS isx_request_id,
    NULL AS isx_unit_id,
    NULL AS isx_request_scene,
    NULL AS isx_network,
    NULL AS max_request_id,
    NULL AS max_placement_id,
    NULL AS floor_price,
    NULL AS max_price,
    NULL AS lib_net_status,
    NULL AS err_msg,
    NULL AS err_type,
    NULL AS ga_session_number,
    NULL AS level_id,
    NULL AS entrance,
    NULL AS can_reive,
    NULL AS was_revive,
    0 AS adslog_request_step_cnt,
    0 AS lib_isx_max_request_step_cnt,
    1 AS total_latency_step_cnt,
    0 AS qualified_latency_step_cnt,
    latency_event_cnt,
    0 AS qualified_latency_event_cnt,
    NULL AS qualified_latency_status_set
  FROM latency_total_events

  UNION ALL

  SELECT
    user_pseudo_id,
    'B' AS experiment_group,
    'screw_puzzle' AS product,
    NULL AS country,
    qualified_latency_ts AS event_timestamp,
    'latency_isadx_qualified' AS event_name,
    CONCAT(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S', TIMESTAMP_MICROS(qualified_latency_ts), 'UTC'), ' UTC') AS utc_datetime,
    NULL AS conf_id,
    NULL AS load_method,
    NULL AS scene,
    NULL AS diff_prev_request_sec,
    ad_format,
    NULL AS duration,
    NULL AS cache,
    NULL AS request_type,
    request_id,
    NULL AS max_unit_id,
    NULL AS request_scene,
    NULL AS step,
    NULL AS filled_value,
    NULL AS network_name,
    NULL AS network_placement,
    NULL AS isx_request_id,
    NULL AS isx_unit_id,
    NULL AS isx_request_scene,
    NULL AS isx_network,
    NULL AS max_request_id,
    NULL AS max_placement_id,
    NULL AS floor_price,
    NULL AS max_price,
    NULL AS lib_net_status,
    NULL AS err_msg,
    NULL AS err_type,
    NULL AS ga_session_number,
    NULL AS level_id,
    NULL AS entrance,
    NULL AS can_reive,
    NULL AS was_revive,
    0 AS adslog_request_step_cnt,
    0 AS lib_isx_max_request_step_cnt,
    0 AS total_latency_step_cnt,
    1 AS qualified_latency_step_cnt,
    0 AS latency_event_cnt,
    qualified_latency_event_cnt,
    qualified_latency_status_set
  FROM latency_qualified_events
),
final_timeline AS (
  SELECT
    *,
    SUM(adslog_request_step_cnt) OVER (
      ORDER BY event_timestamp, request_id, max_request_id, event_name
    ) AS adslog_request_cum_cnt,
    SUM(lib_isx_max_request_step_cnt) OVER (
      ORDER BY event_timestamp, request_id, max_request_id, event_name
    ) AS lib_isx_max_request_cum_cnt,
    SUM(total_latency_step_cnt) OVER (
      ORDER BY event_timestamp, request_id, max_request_id, event_name
    ) AS total_latency_cum_cnt,
    SUM(qualified_latency_step_cnt) OVER (
      ORDER BY event_timestamp, request_id, max_request_id, event_name
    ) AS qualified_latency_cum_cnt
  FROM merged
)
SELECT
  *,
  SAFE_DIVIDE(lib_isx_max_request_cum_cnt, NULLIF(adslog_request_cum_cnt, 0)) AS max_request_ratio_cum,
  SAFE_DIVIDE(qualified_latency_cum_cnt, NULLIF(total_latency_cum_cnt, 0)) AS latency_ratio_cum,
  SAFE_DIVIDE(qualified_latency_cum_cnt, NULLIF(total_latency_cum_cnt, 0))
    - SAFE_DIVIDE(lib_isx_max_request_cum_cnt, NULLIF(adslog_request_cum_cnt, 0)) AS ratio_gap_cum
FROM final_timeline
ORDER BY event_timestamp, request_id, max_request_id, event_name;
