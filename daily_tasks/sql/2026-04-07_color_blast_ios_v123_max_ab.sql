-- 查询目的：
-- 1. 只看 ios_color_blast 的 app_info.version = 1.23.0
-- 2. AB 范围使用所有 interstitial* 事件确定
-- 3. 输出 MAX 口径的分天、分组展示与收入
--
-- 输出字段：
-- dt, ab_group, impression_pv, revenue_usd, ecpm

WITH all_interstitial_events AS (
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
  WHERE event_date BETWEEN '2026-03-22' AND '2026-04-07'
    AND event_name IS NOT NULL
    AND STARTS_WITH(event_name, 'interstitial')
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
      ELSE NULL
    END AS ab_group,
    MIN(event_timestamp) AS min_event_ts_utc,
    MAX(event_timestamp) AS max_event_ts_utc
  FROM all_interstitial_events
  WHERE normalized_conf_id IN ('4445_A', '4447_B')
  GROUP BY user_id, ab_group
)
SELECT
  FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS dt,
  windows.ab_group,
  COUNT(*) AS impression_pv,
  SUM(max_rows.Revenue) AS revenue_usd,
  SAFE_DIVIDE(SUM(max_rows.Revenue) * 1000.0, COUNT(*)) AS ecpm
FROM `gpdata-224001.applovin_max.ios_color_blast_*` AS max_rows
JOIN ab_windows AS windows
  ON max_rows.User_ID = windows.user_id
 AND UNIX_MICROS(max_rows.`Date`) BETWEEN windows.min_event_ts_utc AND windows.max_event_ts_utc
WHERE _TABLE_SUFFIX BETWEEN '20260323' AND '20260405'
  AND max_rows.User_ID IS NOT NULL
  AND UPPER(max_rows.Ad_Format) = 'INTER'
  AND max_rows.`Date` >= TIMESTAMP('2026-03-23 00:00:00+00')
  AND max_rows.`Date` < TIMESTAMP('2026-04-07 00:00:00+00')
GROUP BY dt, windows.ab_group
ORDER BY dt, ab_group;
