WITH hudi_channel_daily AS (
  SELECT
    'screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('AppLovin', 'APPLOVIN_NETWORK') THEN 'AppLovin'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') = 'APPLOVIN_EXCHANGE' THEN 'APPLOVIN_EXCHANGE'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Pangle', 'TIKTOK_BIDDING') THEN 'Pangle'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Unity Ads', 'UNITY_BIDDING') THEN 'Unity Ads'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Moloco', 'MOLOCO_BIDDING') THEN 'Moloco'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Facebook', 'FACEBOOK_NETWORK') THEN 'Facebook'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('InMobi', 'INMOBI_BIDDING') THEN 'InMobi'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Mintegral', 'MINTEGRAL_BIDDING') THEN 'Mintegral'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('ironSource', 'IRONSOURCE_BIDDING') THEN 'ironSource'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('BidMachine', 'BIDMACHINE_BIDDING') THEN 'BidMachine'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Ogury', 'OGURY_PRESAGE_BIDDING') THEN 'Ogury'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Amazon Ad Marketplace', 'AMAZON_MARKETPLACE_NETWORK') THEN 'Amazon Ad Marketplace'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Google Ad Manager', 'GOOGLE_AD_MANAGER_NETWORK') THEN 'Google Ad Manager'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('PubMatic', 'PUBMATIC_BIDDING') THEN 'PubMatic'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('HyprMX', 'HYPRMX_NETWORK') THEN 'HyprMX'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('LINE', 'LINE_NETWORK') THEN 'LINE'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Yandex', 'YANDEX_BIDDING') THEN 'Yandex'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('BIGO Ads', 'BIGO_BIDDING') THEN 'BIGO Ads'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Chartboost', 'CHARTBOOST_BIDDING', 'CHARTBOOST_NETWORK') THEN 'Chartboost'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Google AdMob', 'ADMOB_BIDDING', 'ADMOB_NETWORK') THEN 'Google AdMob'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Liftoff Monetize', 'VUNGLE_BIDDING') THEN 'Liftoff Monetize'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('DT Exchange', 'FYBER_BIDDING') THEN 'DT Exchange'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('IsAdxCustomAdapter', 'Liftoff_custom', 'TpAdxCustomAdapter', 'MaticooCustomAdapter', 'CUSTOM_NETWORK_SDK') THEN 'CUSTOM_NETWORK_SDK'
      ELSE 'UNMAPPED'
    END AS mapped_channel,
    COUNTIF(event_name IN ('interstitial_ad_impression', 'reward_ad_impression')) AS hudi_impression_pv
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name IN ('interstitial_ad_impression', 'reward_ad_impression')
  GROUP BY product, event_date, ad_format, mapped_channel

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS event_date,
    CASE
      WHEN event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('AppLovin', 'APPLOVIN_NETWORK') THEN 'AppLovin'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') = 'APPLOVIN_EXCHANGE' THEN 'APPLOVIN_EXCHANGE'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Pangle', 'TIKTOK_BIDDING') THEN 'Pangle'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Unity Ads', 'UNITY_BIDDING') THEN 'Unity Ads'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Moloco', 'MOLOCO_BIDDING') THEN 'Moloco'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Facebook', 'FACEBOOK_NETWORK') THEN 'Facebook'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('InMobi', 'INMOBI_BIDDING') THEN 'InMobi'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Mintegral', 'MINTEGRAL_BIDDING') THEN 'Mintegral'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('ironSource', 'IRONSOURCE_BIDDING') THEN 'ironSource'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('BidMachine', 'BIDMACHINE_BIDDING') THEN 'BidMachine'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Ogury', 'OGURY_PRESAGE_BIDDING') THEN 'Ogury'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Amazon Ad Marketplace', 'AMAZON_MARKETPLACE_NETWORK') THEN 'Amazon Ad Marketplace'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Google Ad Manager', 'GOOGLE_AD_MANAGER_NETWORK') THEN 'Google Ad Manager'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('PubMatic', 'PUBMATIC_BIDDING') THEN 'PubMatic'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('HyprMX', 'HYPRMX_NETWORK') THEN 'HyprMX'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('LINE', 'LINE_NETWORK') THEN 'LINE'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Yandex', 'YANDEX_BIDDING') THEN 'Yandex'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('BIGO Ads', 'BIGO_BIDDING') THEN 'BIGO Ads'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Chartboost', 'CHARTBOOST_BIDDING', 'CHARTBOOST_NETWORK') THEN 'Chartboost'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Google AdMob', 'ADMOB_BIDDING', 'ADMOB_NETWORK') THEN 'Google AdMob'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('Liftoff Monetize', 'VUNGLE_BIDDING') THEN 'Liftoff Monetize'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('DT Exchange', 'FYBER_BIDDING') THEN 'DT Exchange'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'network_name') IN ('IsAdxCustomAdapter', 'Liftoff_custom', 'TpAdxCustomAdapter', 'MaticooCustomAdapter', 'CUSTOM_NETWORK_SDK') THEN 'CUSTOM_NETWORK_SDK'
      ELSE 'UNMAPPED'
    END AS mapped_channel,
    COUNTIF(event_name IN ('interstitial_ad_impression', 'reward_ad_impression')) AS hudi_impression_pv
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name IN ('interstitial_ad_impression', 'reward_ad_impression')
  GROUP BY product, event_date, ad_format, mapped_channel
),
max_channel_daily AS (
  SELECT
    'screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    CASE
      WHEN UPPER(max_rows.Network) IN ('APPLOVIN', 'APPLOVIN_NETWORK') THEN 'AppLovin'
      WHEN UPPER(max_rows.Network) = 'APPLOVIN_EXCHANGE' THEN 'APPLOVIN_EXCHANGE'
      WHEN UPPER(max_rows.Network) IN ('PANGLE', 'TIKTOK_BIDDING') THEN 'Pangle'
      WHEN UPPER(max_rows.Network) IN ('UNITY ADS', 'UNITY_BIDDING') THEN 'Unity Ads'
      WHEN UPPER(max_rows.Network) IN ('MOLOCO', 'MOLOCO_BIDDING') THEN 'Moloco'
      WHEN UPPER(max_rows.Network) IN ('FACEBOOK', 'FACEBOOK_NETWORK') THEN 'Facebook'
      WHEN UPPER(max_rows.Network) IN ('INMOBI', 'INMOBI_BIDDING') THEN 'InMobi'
      WHEN UPPER(max_rows.Network) IN ('MINTEGRAL', 'MINTEGRAL_BIDDING') THEN 'Mintegral'
      WHEN UPPER(max_rows.Network) IN ('IRONSOURCE', 'IRONSOURCE_BIDDING') THEN 'ironSource'
      WHEN UPPER(max_rows.Network) IN ('BIDMACHINE', 'BIDMACHINE_BIDDING') THEN 'BidMachine'
      WHEN UPPER(max_rows.Network) IN ('OGURY', 'OGURY_PRESAGE_BIDDING') THEN 'Ogury'
      WHEN UPPER(max_rows.Network) IN ('AMAZON AD MARKETPLACE', 'AMAZON_MARKETPLACE_NETWORK') THEN 'Amazon Ad Marketplace'
      WHEN UPPER(max_rows.Network) IN ('GOOGLE AD MANAGER', 'GOOGLE_AD_MANAGER_NETWORK') THEN 'Google Ad Manager'
      WHEN UPPER(max_rows.Network) IN ('PUBMATIC', 'PUBMATIC_BIDDING') THEN 'PubMatic'
      WHEN UPPER(max_rows.Network) IN ('HYPRMX', 'HYPRMX_NETWORK') THEN 'HyprMX'
      WHEN UPPER(max_rows.Network) IN ('LINE', 'LINE_NETWORK') THEN 'LINE'
      WHEN UPPER(max_rows.Network) IN ('YANDEX', 'YANDEX_BIDDING') THEN 'Yandex'
      WHEN UPPER(max_rows.Network) IN ('BIGO ADS', 'BIGO_BIDDING') THEN 'BIGO Ads'
      WHEN UPPER(max_rows.Network) IN ('CHARTBOOST', 'CHARTBOOST_BIDDING', 'CHARTBOOST_NETWORK') THEN 'Chartboost'
      WHEN UPPER(max_rows.Network) IN ('GOOGLE ADMOB', 'ADMOB_BIDDING', 'ADMOB_NETWORK') THEN 'Google AdMob'
      WHEN UPPER(max_rows.Network) IN ('LIFTOFF MONETIZE', 'VUNGLE_BIDDING') THEN 'Liftoff Monetize'
      WHEN UPPER(max_rows.Network) IN ('DT EXCHANGE', 'FYBER_BIDDING') THEN 'DT Exchange'
      WHEN UPPER(max_rows.Network) IN ('ISADXCUSTOMADAPTER', 'LIFTOFF_CUSTOM', 'TPADXCUSTOMADAPTER', 'MATICOOCUSTOMADAPTER', 'CUSTOM_NETWORK_SDK') THEN 'CUSTOM_NETWORK_SDK'
      ELSE 'UNMAPPED'
    END AS mapped_channel,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '{{ table_suffix_start }}' AND '{{ table_suffix_end }}'
    AND max_rows.User_ID IS NOT NULL
  GROUP BY product, event_date, ad_format, mapped_channel

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    FORMAT_DATE('%Y-%m-%d', DATE(max_rows.`Date`, 'UTC')) AS event_date,
    CASE
      WHEN UPPER(max_rows.Ad_Format) LIKE 'INTER%' THEN 'interstitial'
      WHEN UPPER(max_rows.Ad_Format) LIKE 'REWARD%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    CASE
      WHEN UPPER(max_rows.Network) IN ('APPLOVIN', 'APPLOVIN_NETWORK') THEN 'AppLovin'
      WHEN UPPER(max_rows.Network) = 'APPLOVIN_EXCHANGE' THEN 'APPLOVIN_EXCHANGE'
      WHEN UPPER(max_rows.Network) IN ('PANGLE', 'TIKTOK_BIDDING') THEN 'Pangle'
      WHEN UPPER(max_rows.Network) IN ('UNITY ADS', 'UNITY_BIDDING') THEN 'Unity Ads'
      WHEN UPPER(max_rows.Network) IN ('MOLOCO', 'MOLOCO_BIDDING') THEN 'Moloco'
      WHEN UPPER(max_rows.Network) IN ('FACEBOOK', 'FACEBOOK_NETWORK') THEN 'Facebook'
      WHEN UPPER(max_rows.Network) IN ('INMOBI', 'INMOBI_BIDDING') THEN 'InMobi'
      WHEN UPPER(max_rows.Network) IN ('MINTEGRAL', 'MINTEGRAL_BIDDING') THEN 'Mintegral'
      WHEN UPPER(max_rows.Network) IN ('IRONSOURCE', 'IRONSOURCE_BIDDING') THEN 'ironSource'
      WHEN UPPER(max_rows.Network) IN ('BIDMACHINE', 'BIDMACHINE_BIDDING') THEN 'BidMachine'
      WHEN UPPER(max_rows.Network) IN ('OGURY', 'OGURY_PRESAGE_BIDDING') THEN 'Ogury'
      WHEN UPPER(max_rows.Network) IN ('AMAZON AD MARKETPLACE', 'AMAZON_MARKETPLACE_NETWORK') THEN 'Amazon Ad Marketplace'
      WHEN UPPER(max_rows.Network) IN ('GOOGLE AD MANAGER', 'GOOGLE_AD_MANAGER_NETWORK') THEN 'Google Ad Manager'
      WHEN UPPER(max_rows.Network) IN ('PUBMATIC', 'PUBMATIC_BIDDING') THEN 'PubMatic'
      WHEN UPPER(max_rows.Network) IN ('HYPRMX', 'HYPRMX_NETWORK') THEN 'HyprMX'
      WHEN UPPER(max_rows.Network) IN ('LINE', 'LINE_NETWORK') THEN 'LINE'
      WHEN UPPER(max_rows.Network) IN ('YANDEX', 'YANDEX_BIDDING') THEN 'Yandex'
      WHEN UPPER(max_rows.Network) IN ('BIGO ADS', 'BIGO_BIDDING') THEN 'BIGO Ads'
      WHEN UPPER(max_rows.Network) IN ('CHARTBOOST', 'CHARTBOOST_BIDDING', 'CHARTBOOST_NETWORK') THEN 'Chartboost'
      WHEN UPPER(max_rows.Network) IN ('GOOGLE ADMOB', 'ADMOB_BIDDING', 'ADMOB_NETWORK') THEN 'Google AdMob'
      WHEN UPPER(max_rows.Network) IN ('LIFTOFF MONETIZE', 'VUNGLE_BIDDING') THEN 'Liftoff Monetize'
      WHEN UPPER(max_rows.Network) IN ('DT EXCHANGE', 'FYBER_BIDDING') THEN 'DT Exchange'
      WHEN UPPER(max_rows.Network) IN ('ISADXCUSTOMADAPTER', 'LIFTOFF_CUSTOM', 'TPADXCUSTOMADAPTER', 'MATICOOCUSTOMADAPTER', 'CUSTOM_NETWORK_SDK') THEN 'CUSTOM_NETWORK_SDK'
      ELSE 'UNMAPPED'
    END AS mapped_channel,
    COUNT(*) AS max_impression_pv
  FROM `gpdata-224001.applovin_max.ios_screw_puzzle_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN '{{ table_suffix_start }}' AND '{{ table_suffix_end }}'
    AND max_rows.User_ID IS NOT NULL
  GROUP BY product, event_date, ad_format, mapped_channel
)
SELECT
  COALESCE(h.product, m.product) AS product,
  COALESCE(h.event_date, m.event_date) AS event_date,
  COALESCE(h.ad_format, m.ad_format) AS ad_format,
  COALESCE(h.mapped_channel, m.mapped_channel) AS mapped_channel,
  COALESCE(h.hudi_impression_pv, 0) AS hudi_impression_pv,
  COALESCE(m.max_impression_pv, 0) AS max_impression_pv,
  SAFE_DIVIDE(COALESCE(h.hudi_impression_pv, 0), COALESCE(m.max_impression_pv, 0)) AS hudi_max_rate
FROM hudi_channel_daily AS h
FULL OUTER JOIN max_channel_daily AS m
  ON h.product = m.product
 AND h.event_date = m.event_date
 AND h.ad_format = m.ad_format
 AND h.mapped_channel = m.mapped_channel
WHERE COALESCE(h.ad_format, m.ad_format) IS NOT NULL
ORDER BY product, ad_format, mapped_channel, event_date
