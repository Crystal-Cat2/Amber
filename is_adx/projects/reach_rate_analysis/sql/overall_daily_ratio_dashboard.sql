WITH hudi_events AS (
  SELECT
    'screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    event_name
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression',
      'interstitial_ad_display_failed', 'reward_ad_display_faile'
    )

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    event_name
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression',
      'interstitial_ad_display_failed', 'reward_ad_display_faile'
    )
),
hudi_daily AS (
  SELECT
    product,
    event_date,
    ad_format,
    COUNTIF(event_name IN ('interstitial_ad_show', 'reward_ad_show')) AS show_pv,
    COUNTIF(event_name IN ('interstitial_ad_impression', 'reward_ad_impression')) AS impression_pv,
    COUNTIF(event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile')) AS display_failed_pv
  FROM hudi_events
  WHERE ad_format IS NOT NULL
  GROUP BY product, event_date, ad_format
),
max_daily AS (
  SELECT
    'screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '{{ table_suffix_start }}' AND '{{ table_suffix_end }}'
    AND max_rows.User_ID IS NOT NULL
    AND UPPER(max_rows.Ad_Format) IN ('INTER', 'INTERSTITIAL', 'REWARD', 'REWARDED')
  GROUP BY product, event_date, ad_format

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '{{ table_suffix_start }}' AND '{{ table_suffix_end }}'
    AND max_rows.User_ID IS NOT NULL
    AND UPPER(max_rows.Ad_Format) IN ('INTER', 'INTERSTITIAL', 'REWARD', 'REWARDED')
  GROUP BY product, event_date, ad_format
)
SELECT
  COALESCE(hudi_daily.product, max_daily.product) AS product,
  COALESCE(hudi_daily.event_date, max_daily.event_date) AS event_date,
  COALESCE(hudi_daily.ad_format, max_daily.ad_format) AS ad_format,
  COALESCE(hudi_daily.show_pv, 0) AS show_pv,
  COALESCE(hudi_daily.impression_pv, 0) AS impression_pv,
  COALESCE(hudi_daily.display_failed_pv, 0) AS display_failed_pv,
  COALESCE(max_daily.max_impression_pv, 0) AS max_impression_pv,
  SAFE_DIVIDE(COALESCE(hudi_daily.impression_pv, 0), COALESCE(hudi_daily.show_pv, 0)) AS impression_show_rate,
  SAFE_DIVIDE(
    COALESCE(hudi_daily.impression_pv, 0) + COALESCE(hudi_daily.display_failed_pv, 0),
    COALESCE(hudi_daily.show_pv, 0)
  ) AS impression_plus_failed_show_rate,
  SAFE_DIVIDE(COALESCE(hudi_daily.impression_pv, 0), COALESCE(max_daily.max_impression_pv, 0)) AS hudi_max_rate
FROM hudi_daily
FULL OUTER JOIN max_daily
  ON hudi_daily.product = max_daily.product
 AND hudi_daily.event_date = max_daily.event_date
 AND hudi_daily.ad_format = max_daily.ad_format
WHERE COALESCE(hudi_daily.ad_format, max_daily.ad_format) IS NOT NULL
ORDER BY product, ad_format, event_date
