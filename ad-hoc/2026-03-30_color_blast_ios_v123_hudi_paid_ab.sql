-- 查询目的：
-- 1. 只看 ios_color_blast 的 app_info.version = 1.23.0
-- 2. AB 范围使用所有 interstitial* 事件确定
-- 3. 输出 overall 与 classic_revive_back 两层结果
-- 4. trigger / show / hudi_impression 来自 Hudi
-- 5. 分场景展示与收入使用 Hudi paid 事件：
--    paid_pv = interstitial_ad_paid 条数
--    paid_revenue_usd = revenue / 1e6
--    paid_ecpm = paid_revenue_usd * 1000 / paid_pv
--
-- 输出字段：
-- dt, scope_key, ab_group,
-- trigger_pv, show_pv, show_rate,
-- hudi_impression_pv, paid_pv, paid_revenue_usd, paid_ecpm

WITH all_interstitial_events AS (
  SELECT
    user_id,
    event_name,
    event_timestamp,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS dt,
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
    ) AS scene_key,
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
    ) AS normalized_conf_id,
    COALESCE(
      (
        SELECT SAFE_CAST(ep.value.int_value AS FLOAT64)
        FROM UNNEST(event_params.array) AS ep
        WHERE ep.key = 'revenue'
        LIMIT 1
      ),
      (
        SELECT SAFE_CAST(ep.value.double_value AS FLOAT64)
        FROM UNNEST(event_params.array) AS ep
        WHERE ep.key = 'revenue'
        LIMIT 1
      ),
      (
        SELECT SAFE_CAST(ep.value.float_value AS FLOAT64)
        FROM UNNEST(event_params.array) AS ep
        WHERE ep.key = 'revenue'
        LIMIT 1
      ),
      (
        SELECT SAFE_CAST(ep.value.string_value AS FLOAT64)
        FROM UNNEST(event_params.array) AS ep
        WHERE ep.key = 'revenue'
        LIMIT 1
      )
    ) / 1000000.0 AS paid_revenue_usd
  FROM `transferred.hudi_ods.ios_color_blast`
  WHERE event_date BETWEEN '2026-03-22' AND '2026-03-31'
    AND event_name IS NOT NULL
    AND STARTS_WITH(event_name, 'interstitial')
    AND user_id IS NOT NULL
    AND app_info.version = '1.23.0'
    AND TIMESTAMP_MICROS(event_timestamp) >= TIMESTAMP('2026-03-23 00:00:00+00')
    AND TIMESTAMP_MICROS(event_timestamp) < TIMESTAMP('2026-03-31 00:00:00+00')
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
),
scoped_events AS (
  SELECT
    e.dt,
    w.ab_group,
    'overall' AS scope_key,
    e.event_name,
    e.paid_revenue_usd
  FROM all_interstitial_events AS e
  JOIN ab_windows AS w
    ON e.user_id = w.user_id
   AND e.event_timestamp BETWEEN w.min_event_ts_utc AND w.max_event_ts_utc

  UNION ALL

  SELECT
    e.dt,
    w.ab_group,
    'classic_revive_back' AS scope_key,
    e.event_name,
    e.paid_revenue_usd
  FROM all_interstitial_events AS e
  JOIN ab_windows AS w
    ON e.user_id = w.user_id
   AND e.event_timestamp BETWEEN w.min_event_ts_utc AND w.max_event_ts_utc
  WHERE e.scene_key = 'classic_revive_back'
),
daily AS (
  SELECT
    dt,
    scope_key,
    ab_group,
    COUNTIF(event_name = 'interstitial_ad_trigger') AS trigger_pv,
    COUNTIF(event_name = 'interstitial_ad_show') AS show_pv,
    COUNTIF(event_name = 'interstitial_ad_impression') AS hudi_impression_pv,
    COUNTIF(event_name = 'interstitial_ad_paid') AS paid_pv,
    SUM(IF(event_name = 'interstitial_ad_paid', paid_revenue_usd, 0.0)) AS paid_revenue_usd,
    SAFE_DIVIDE(
      COUNTIF(event_name = 'interstitial_ad_show'),
      NULLIF(COUNTIF(event_name = 'interstitial_ad_trigger'), 0)
    ) AS show_rate,
    SAFE_DIVIDE(
      SUM(IF(event_name = 'interstitial_ad_paid', paid_revenue_usd, 0.0)) * 1000.0,
      NULLIF(COUNTIF(event_name = 'interstitial_ad_paid'), 0)
    ) AS paid_ecpm
  FROM scoped_events
  GROUP BY dt, scope_key, ab_group
),
total AS (
  SELECT
    'TOTAL' AS dt,
    scope_key,
    ab_group,
    SUM(trigger_pv) AS trigger_pv,
    SUM(show_pv) AS show_pv,
    SAFE_DIVIDE(SUM(show_pv), NULLIF(SUM(trigger_pv), 0)) AS show_rate,
    SUM(hudi_impression_pv) AS hudi_impression_pv,
    SUM(paid_pv) AS paid_pv,
    SUM(paid_revenue_usd) AS paid_revenue_usd,
    SAFE_DIVIDE(SUM(paid_revenue_usd) * 1000.0, NULLIF(SUM(paid_pv), 0)) AS paid_ecpm
  FROM daily
  GROUP BY scope_key, ab_group
)
SELECT *
FROM daily

UNION ALL

SELECT *
FROM total

ORDER BY scope_key, ab_group, dt;
