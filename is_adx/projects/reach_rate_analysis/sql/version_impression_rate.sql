-- 按版本统计 impression/show 比例，只看 TOP3 版本
WITH version_events AS (
  SELECT
    'screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    app_info.version AS app_version,
    event_name
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2025-08-01' AND '2026-03-28'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression'
    )
    AND app_info.version IS NOT NULL

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    app_info.version AS app_version,
    event_name
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2025-08-01' AND '2026-03-28'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression'
    )
    AND app_info.version IS NOT NULL
),
top_versions AS (
  SELECT
    product,
    app_version,
    COUNT(*) AS total_events
  FROM version_events
  GROUP BY product, app_version
  ORDER BY total_events DESC
  LIMIT 3
)
SELECT
  ve.product,
  FORMAT_DATE('%Y-%m', ve.event_date) AS month,
  ve.app_version,
  COUNTIF(ve.event_name IN ('interstitial_ad_show', 'reward_ad_show')) AS show_pv,
  COUNTIF(ve.event_name IN ('interstitial_ad_impression', 'reward_ad_impression')) AS impression_pv,
  SAFE_DIVIDE(
    COUNTIF(ve.event_name IN ('interstitial_ad_impression', 'reward_ad_impression')),
    COUNTIF(ve.event_name IN ('interstitial_ad_show', 'reward_ad_show'))
  ) AS imp_show_rate
FROM version_events ve
INNER JOIN top_versions tv
  ON ve.product = tv.product
 AND ve.app_version = tv.app_version
GROUP BY product, month, app_version
ORDER BY product, app_version, month
