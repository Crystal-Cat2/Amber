-- execution_path = local_bigquery
-- Query goal:
-- 1. Use userProperty firebase_exp_5 (0=A, 1=B) to split screw_puzzle Android + iOS AB.
-- 2. MAX source: all networks, joined via user_id for both platforms.
-- 3. ULP source: IronSource data from external_ad_ironsource_data,
--    Android joined via advertising_id, iOS joined via user_id.
-- 4. Output carries source + is_ironsource flags; Python filters at aggregation time.

DECLARE start_date DATE DEFAULT DATE '2026-04-02';
DECLARE end_date DATE DEFAULT CURRENT_DATE('UTC');

WITH experiment_groups AS (
  SELECT product, user_id, advertising_id, experiment_group
  FROM (
    -- Android
    SELECT
      'screw_puzzle' AS product,
      user_id,
      device.advertising_id,
      CASE
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'firebase_exp_5' LIMIT 1)
        WHEN '0' THEN 'A'
        WHEN '1' THEN 'B'
      END AS experiment_group
    FROM `transferred.hudi_ods.screw_puzzle`
    WHERE event_date BETWEEN FORMAT_DATE('%Y-%m-%d', start_date) AND FORMAT_DATE('%Y-%m-%d', end_date)
      AND user_id IS NOT NULL
      AND event_timestamp >= UNIX_MICROS(TIMESTAMP '2026-04-02 08:00:00 UTC')

    UNION ALL

    -- iOS
    SELECT
      'ios_screw_puzzle' AS product,
      user_id,
      CAST(NULL AS STRING) AS advertising_id,
      CASE
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'firebase_exp_5' LIMIT 1)
        WHEN '0' THEN 'A'
        WHEN '1' THEN 'B'
      END AS experiment_group
    FROM `transferred.hudi_ods.ios_screw_puzzle`
    WHERE event_date BETWEEN FORMAT_DATE('%Y-%m-%d', start_date) AND FORMAT_DATE('%Y-%m-%d', end_date)
      AND user_id IS NOT NULL
      AND event_timestamp >= UNIX_MICROS(TIMESTAMP '2026-04-02 08:00:00 UTC')
  )
  WHERE experiment_group IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product, user_id
    ORDER BY advertising_id DESC
  ) = 1
),

-- MAX Android: all networks
max_android AS (
  SELECT
    'screw_puzzle' AS product,
    DATE(`Date`, 'UTC') AS date,
    User_ID AS user_id,
    UPPER(Ad_Format) AS ad_format,
    Network AS network,
    LOWER(Ad_placement) LIKE 'ironsourcecustom%' AS is_ironsource,
    1 AS impression,
    Revenue AS revenue,
    'max' AS source
  FROM `gpdata-224001.applovin_max.screw_puzzle_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
    AND Revenue IS NOT NULL
),
matched_max_android AS (
  SELECT
    s.date, s.product, s.ad_format, w.experiment_group,
    s.network, s.source, s.is_ironsource, s.impression, s.revenue
  FROM max_android AS s
  JOIN experiment_groups AS w
    ON w.product = 'screw_puzzle'
   AND s.user_id = w.user_id
),

-- MAX iOS: all networks
max_ios AS (
  SELECT
    'ios_screw_puzzle' AS product,
    DATE(`Date`, 'UTC') AS date,
    User_ID AS user_id,
    UPPER(Ad_Format) AS ad_format,
    Network AS network,
    LOWER(Ad_placement) LIKE 'ironsourcecustom%' AS is_ironsource,
    1 AS impression,
    Revenue AS revenue,
    'max' AS source
  FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*`
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
    AND User_ID IS NOT NULL
    AND `Date` >= TIMESTAMP(start_date)
    AND `Date` < TIMESTAMP(DATE_ADD(end_date, INTERVAL 1 DAY))
    AND Revenue IS NOT NULL
),
matched_max_ios AS (
  SELECT
    s.date, s.product, s.ad_format, w.experiment_group,
    s.network, s.source, s.is_ironsource, s.impression, s.revenue
  FROM max_ios AS s
  JOIN experiment_groups AS w
    ON w.product = 'ios_screw_puzzle'
   AND s.user_id = w.user_id
),

-- ULP Android: IronSource via advertising_id
ulp_android AS (
  SELECT
    'screw_puzzle' AS product,
    date,
    advertising_id,
    CAST(NULL AS STRING) AS user_id,
    CASE UPPER(ad_unit)
      WHEN 'INTERSTITIAL' THEN 'INTER'
      WHEN 'REWARDED_VIDEO' THEN 'REWARD'
      ELSE UPPER(ad_unit)
    END AS ad_format,
    'IronSource' AS network,
    TRUE AS is_ironsource,
    1 AS impression,
    revenue,
    'ulp' AS source
  FROM `transferred.dw_external_data.external_ad_ironsource_data`
  WHERE slim_name = 'screw_puzzle'
    AND date BETWEEN start_date AND end_date
    AND DATE_DIFF(run_date, date, DAY) = 1
    AND advertising_id IS NOT NULL
    AND advertising_id != ''
    AND revenue IS NOT NULL
),
matched_ulp_android AS (
  SELECT
    s.date, s.product, s.ad_format, w.experiment_group,
    s.network, s.source, s.is_ironsource, s.impression, s.revenue
  FROM ulp_android AS s
  JOIN experiment_groups AS w
    ON w.product = 'screw_puzzle'
   AND s.advertising_id = w.advertising_id
   AND w.advertising_id IS NOT NULL
   AND w.advertising_id != ''
),

-- ULP iOS: IronSource via user_id
ulp_ios AS (
  SELECT
    'ios_screw_puzzle' AS product,
    date,
    CAST(NULL AS STRING) AS advertising_id,
    user_id,
    CASE UPPER(ad_unit)
      WHEN 'INTERSTITIAL' THEN 'INTER'
      WHEN 'REWARDED_VIDEO' THEN 'REWARD'
      ELSE UPPER(ad_unit)
    END AS ad_format,
    'IronSource' AS network,
    TRUE AS is_ironsource,
    1 AS impression,
    revenue,
    'ulp' AS source
  FROM `transferred.dw_external_data.external_ad_ironsource_data`
  WHERE slim_name = 'ios_screw_puzzle'
    AND date BETWEEN start_date AND end_date
    AND DATE_DIFF(run_date, date, DAY) = 1
    AND user_id IS NOT NULL
    AND user_id != ''
    AND revenue IS NOT NULL
),
matched_ulp_ios AS (
  SELECT
    s.date, s.product, s.ad_format, w.experiment_group,
    s.network, s.source, s.is_ironsource, s.impression, s.revenue
  FROM ulp_ios AS s
  JOIN experiment_groups AS w
    ON w.product = 'ios_screw_puzzle'
   AND s.user_id = w.user_id
),

combined AS (
  SELECT date, product, ad_format, experiment_group, network, source, is_ironsource, impression, revenue FROM matched_max_android
  UNION ALL
  SELECT date, product, ad_format, experiment_group, network, source, is_ironsource, impression, revenue FROM matched_max_ios
  UNION ALL
  SELECT date, product, ad_format, experiment_group, network, source, is_ironsource, impression, revenue FROM matched_ulp_android
  UNION ALL
  SELECT date, product, ad_format, experiment_group, network, source, is_ironsource, impression, revenue FROM matched_ulp_ios
)

SELECT
  date,
  product,
  ad_format,
  experiment_group,
  source,
  is_ironsource,
  SUM(impression) AS impression,
  SUM(revenue) AS revenue,
  SAFE_DIVIDE(SUM(revenue) * 1000.0, NULLIF(SUM(impression), 0)) AS ecpm
FROM combined
GROUP BY date, product, ad_format, experiment_group, source, is_ironsource
ORDER BY date, product, ad_format, experiment_group, source, is_ironsource
