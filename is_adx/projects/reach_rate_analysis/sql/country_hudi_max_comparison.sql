-- 按国家对比 Hudi 和 MAX 的 impression 数据
-- 重点关注美国(US)和印度(IN)在 9-12 月的比例

WITH hudi_country AS (
  SELECT
    'screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    geo.country AS country,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    event_name
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2025-08-01' AND '2026-03-28'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression'
    )
    AND geo.country IN ('United States', 'India')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    geo.country AS country,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    event_name
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2025-08-01' AND '2026-03-28'
    AND event_name IN (
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression'
    )
    AND geo.country IN ('United States', 'India')
),
hudi_daily AS (
  SELECT
    product,
    FORMAT_DATE('%Y-%m-%d', event_date) AS event_date,
    CASE
      WHEN country = 'United States' THEN 'us'
      WHEN country = 'India' THEN 'in'
      ELSE LOWER(country)
    END AS country_code,
    ad_format,
    COUNTIF(event_name IN ('interstitial_ad_show', 'reward_ad_show')) AS show_pv,
    COUNTIF(event_name IN ('interstitial_ad_impression', 'reward_ad_impression')) AS impression_pv
  FROM hudi_country
  WHERE ad_format IS NOT NULL
  GROUP BY product, event_date, country_code, ad_format
),
max_daily AS (
  SELECT
    'screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    max_rows.Country AS country,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '20250801' AND '20260328'
    AND max_rows.User_ID IS NOT NULL
    AND UPPER(max_rows.Ad_Format) IN ('INTER', 'INTERSTITIAL', 'REWARD', 'REWARDED')
    AND max_rows.Country IN ('us', 'in')
  GROUP BY product, event_date, country, ad_format

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    max_rows.Country AS country,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '20250801' AND '20260328'
    AND max_rows.User_ID IS NOT NULL
    AND UPPER(max_rows.Ad_Format) IN ('INTER', 'INTERSTITIAL', 'REWARD', 'REWARDED')
    AND max_rows.Country IN ('us', 'in')
  GROUP BY product, event_date, country, ad_format
)
SELECT
  COALESCE(h.product, m.product) AS product,
  COALESCE(h.event_date, m.event_date) AS event_date,
  UPPER(COALESCE(h.country_code, m.country)) AS country,
  COALESCE(h.ad_format, m.ad_format) AS ad_format,
  COALESCE(h.show_pv, 0) AS hudi_show_pv,
  COALESCE(h.impression_pv, 0) AS hudi_impression_pv,
  COALESCE(m.max_impression_pv, 0) AS max_impression_pv,
  SAFE_DIVIDE(COALESCE(h.impression_pv, 0), COALESCE(m.max_impression_pv, 0)) AS hudi_max_rate
FROM hudi_daily h
FULL OUTER JOIN max_daily m
  ON h.product = m.product
 AND h.event_date = m.event_date
 AND h.country_code = LOWER(m.country)
 AND h.ad_format = m.ad_format
WHERE COALESCE(h.ad_format, m.ad_format) IS NOT NULL
ORDER BY product, event_date, country, ad_format
