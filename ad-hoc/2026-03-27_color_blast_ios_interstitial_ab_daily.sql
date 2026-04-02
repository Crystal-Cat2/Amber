-- 查询目的：
-- 1. 基于 ios_color_blast 的 interstitial conf_id，识别 A/B 两组用户窗口
-- 2. 按 UTC-0 天、按场景输出 Hudi trigger/show 与 show_rate
-- 3. 按 UTC-0 天输出 MAX impression/revenue/eCPM
--
-- 输出字段：
-- report_type, dt, scene_key, ab_group, ab_variant,
-- trigger_pv, show_pv, show_rate,
-- impression_pv, revenue_usd, ecpm,
-- show_rate_diff_vs_b, revenue_diff_vs_b, ecpm_diff_vs_b

WITH anchor_events AS (
  SELECT
    user_id,
    CASE
      WHEN normalized_conf_id = '4445_A' THEN 'A'
      WHEN normalized_conf_id = '4447_B' THEN 'B'
      ELSE NULL
    END AS ab_group,
    MIN(event_timestamp) AS min_event_ts_utc,
    MAX(event_timestamp) AS max_event_ts_utc
  FROM (
    SELECT
      user_id,
      event_timestamp,
      COALESCE(
        SPLIT(
          (
            SELECT ep.value.string_value
            FROM UNNEST(event_params.array) AS ep
            WHERE ep.key = 'placementid_loadmethod_confid'
            LIMIT 1
          ),
          '|'
        )[SAFE_OFFSET(2)],
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params.array) AS ep
          WHERE ep.key = 'conf_id'
          LIMIT 1
        ),
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params.array) AS ep
          WHERE ep.key = 'config_id'
          LIMIT 1
        )
      ) AS normalized_conf_id
    FROM `transferred.hudi_ods.ios_color_blast`
    WHERE event_date BETWEEN '2026-03-22' AND '2026-03-28'
      AND event_name IS NOT NULL
      AND STARTS_WITH(event_name, 'interstitial')
      AND user_id IS NOT NULL
      AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP('2026-03-23 00:00:00+00')
      AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP('2026-03-28 00:00:00+00')
  ) AS scoped_anchor_events
  WHERE normalized_conf_id IN ('4445_A', '4447_B')
  GROUP BY user_id, ab_group
),
hudi_scene_events AS (
  SELECT
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(base.event_timestamp), 'UTC')) AS dt,
    base.user_id,
    base.event_name,
    base.event_timestamp,
    base.scene_key,
    windows.ab_group
  FROM (
    SELECT
      user_id,
      event_name,
      event_timestamp,
      COALESCE(
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params.array) AS ep
          WHERE ep.key IN ('Scene', 'impression_scene')
          LIMIT 1
        ),
        (
          SELECT ep.value.string_value
          FROM UNNEST(event_params.array) AS ep
          WHERE ep.key = 'request_scene'
          LIMIT 1
        ),
        'UNKNOWN'
      ) AS scene_key
    FROM `transferred.hudi_ods.ios_color_blast`
    WHERE event_date BETWEEN '2026-03-22' AND '2026-03-28'
      AND event_name IS NOT NULL
      AND STARTS_WITH(event_name, 'interstitial')
      AND user_id IS NOT NULL
      AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP('2026-03-23 00:00:00+00')
      AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP('2026-03-28 00:00:00+00')
  ) AS base
  JOIN anchor_events AS windows
    ON base.user_id = windows.user_id
   AND base.event_timestamp BETWEEN windows.min_event_ts_utc AND windows.max_event_ts_utc
  WHERE base.event_name IN ('interstitial_ad_trigger', 'interstitial_ad_show')
),
hudi_scene_daily AS (
  SELECT
    dt,
    scene_key,
    ab_group,
    COUNTIF(event_name = 'interstitial_ad_trigger') AS trigger_pv,
    COUNTIF(event_name = 'interstitial_ad_show') AS show_pv
  FROM hudi_scene_events
  GROUP BY dt, scene_key, ab_group
),
hudi_scene_with_diff AS (
  SELECT
    'scene_show_rate' AS report_type,
    dt,
    scene_key,
    ab_group,
    CASE
      WHEN ab_group = 'A' THEN 'csv3_1'
      WHEN ab_group = 'B' THEN 'single_layer'
      ELSE 'unknown'
    END AS ab_label,
    trigger_pv,
    show_pv,
    SAFE_DIVIDE(show_pv, trigger_pv) AS show_rate,
    CAST(NULL AS INT64) AS impression_pv,
    CAST(NULL AS FLOAT64) AS revenue_usd,
    CAST(NULL AS FLOAT64) AS ecpm,
    SAFE_DIVIDE(show_pv, trigger_pv)
      - MAX(IF(ab_group = 'B', SAFE_DIVIDE(show_pv, trigger_pv), NULL))
          OVER (PARTITION BY dt, scene_key) AS show_rate_diff_vs_b,
    CAST(NULL AS FLOAT64) AS revenue_diff_vs_b,
    CAST(NULL AS FLOAT64) AS ecpm_diff_vs_b
  FROM hudi_scene_daily
),
max_daily AS (
  SELECT
    FORMAT_DATE('%Y-%m-%d', DATE(`Date`, 'UTC')) AS dt,
    windows.ab_group,
    COUNT(*) AS impression_pv,
    SUM(Revenue) AS revenue_usd
  FROM `gpdata-224001.applovin_max.ios_color_blast_*` AS max_rows
  JOIN anchor_events AS windows
    ON max_rows.User_ID = windows.user_id
   AND UNIX_MICROS(max_rows.`Date`) BETWEEN windows.min_event_ts_utc AND windows.max_event_ts_utc
  WHERE _TABLE_SUFFIX BETWEEN '20260323' AND '20260327'
    AND max_rows.User_ID IS NOT NULL
    AND UPPER(max_rows.Ad_Format) = 'INTER'
  GROUP BY dt, windows.ab_group
),
max_with_diff AS (
  SELECT
    'overall_monetization' AS report_type,
    dt,
    CAST(NULL AS STRING) AS scene_key,
    ab_group,
    CASE
      WHEN ab_group = 'A' THEN 'csv3_1'
      WHEN ab_group = 'B' THEN 'single_layer'
      ELSE 'unknown'
    END AS ab_label,
    CAST(NULL AS INT64) AS trigger_pv,
    CAST(NULL AS INT64) AS show_pv,
    CAST(NULL AS FLOAT64) AS show_rate,
    impression_pv,
    revenue_usd,
    SAFE_DIVIDE(revenue_usd * 1000.0, impression_pv) AS ecpm,
    CAST(NULL AS FLOAT64) AS show_rate_diff_vs_b,
    revenue_usd - MAX(IF(ab_group = 'B', revenue_usd, NULL)) OVER (PARTITION BY dt) AS revenue_diff_vs_b,
    SAFE_DIVIDE(revenue_usd * 1000.0, impression_pv)
      - MAX(IF(ab_group = 'B', SAFE_DIVIDE(revenue_usd * 1000.0, impression_pv), NULL))
          OVER (PARTITION BY dt) AS ecpm_diff_vs_b
  FROM max_daily
)
SELECT
  report_type,
  dt,
  scene_key,
  ab_group,
  ab_label,
  trigger_pv,
  show_pv,
  show_rate,
  impression_pv,
  revenue_usd,
  ecpm,
  show_rate_diff_vs_b,
  revenue_diff_vs_b,
  ecpm_diff_vs_b
FROM hudi_scene_with_diff

UNION ALL

SELECT
  report_type,
  dt,
  scene_key,
  ab_group,
  ab_label,
  trigger_pv,
  show_pv,
  show_rate,
  impression_pv,
  revenue_usd,
  ecpm,
  show_rate_diff_vs_b,
  revenue_diff_vs_b,
  ecpm_diff_vs_b
FROM max_with_diff

ORDER BY report_type, dt, scene_key, ab_group;
