WITH all_interstitial_events AS (
  SELECT
    user_id,
    event_name,
    event_timestamp,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS dt,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep
       WHERE ep.key IN ('Scene', 'impression_scene') LIMIT 1),
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep
       WHERE ep.key = 'request_scene' LIMIT 1),
      'UNKNOWN'
    ) AS scene_key,
    COALESCE(
      SPLIT(
        (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep
         WHERE ep.key = 'placementid_loadmethod_confid' LIMIT 1),
        '|'
      )[SAFE_OFFSET(2)],
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep
       WHERE ep.key = 'conf_id' LIMIT 1),
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep
       WHERE ep.key = 'config_id' LIMIT 1)
    ) AS normalized_conf_id
  FROM `transferred.hudi_ods.ios_color_blast`
  WHERE event_date BETWEEN '2026-03-22' AND '2026-04-07'
    AND event_name IN ('interstitial_ad_trigger', 'interstitial_ad_show')
    AND user_id IS NOT NULL
    AND app_info.version = '1.23.0'
    AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP('2026-03-23 00:00:00+00')
    AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP('2026-04-07 00:00:00+00')
),
ab_windows AS (
  SELECT
    user_id,
    CASE
      WHEN normalized_conf_id = '4445_A' THEN 'A'
      WHEN normalized_conf_id = '4447_B' THEN 'B'
    END AS ab_group,
    MIN(event_timestamp) AS min_ts,
    MAX(event_timestamp) AS max_ts
  FROM all_interstitial_events
  WHERE normalized_conf_id IN ('4445_A', '4447_B')
  GROUP BY user_id, ab_group
),
matched AS (
  SELECT e.dt, w.ab_group, e.event_name
  FROM all_interstitial_events e
  JOIN ab_windows w
    ON e.user_id = w.user_id
   AND e.event_timestamp BETWEEN w.min_ts AND w.max_ts
  WHERE e.scene_key = 'classic_revive_back'
),
daily AS (
  SELECT
    dt, ab_group,
    COUNTIF(event_name = 'interstitial_ad_trigger') AS trigger_pv,
    COUNTIF(event_name = 'interstitial_ad_show')    AS show_pv,
    SAFE_DIVIDE(
      COUNTIF(event_name = 'interstitial_ad_show'),
      NULLIF(COUNTIF(event_name = 'interstitial_ad_trigger'), 0)
    ) AS show_rate
  FROM matched
  GROUP BY dt, ab_group
),
total AS (
  SELECT
    'TOTAL' AS dt, ab_group,
    SUM(trigger_pv) AS trigger_pv,
    SUM(show_pv)    AS show_pv,
    SAFE_DIVIDE(SUM(show_pv), NULLIF(SUM(trigger_pv), 0)) AS show_rate
  FROM daily
  GROUP BY ab_group
)
SELECT * FROM daily
UNION ALL
SELECT * FROM total
ORDER BY ab_group, dt
