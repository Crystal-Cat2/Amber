WITH experiment_users AS (
  SELECT
    'screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    user_id,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'A' THEN 'no_is_adx'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name = 'lib_isx_group'
  GROUP BY 1, 2, 3, 4
  HAVING experiment_group IS NOT NULL

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    user_id,
    CASE
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'A' THEN 'no_is_adx'
      WHEN (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group') = 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND event_name = 'lib_isx_group'
  GROUP BY 1, 2, 3, 4
  HAVING experiment_group IS NOT NULL
),
scoped_events AS (
  SELECT
    u.product,
    u.event_date,
    u.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COALESCE(
      NULLIF((SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key = 'network_name'), ''),
      '__NO_NETWORK__'
    ) AS network_name,
    COALESCE(
      NULLIF(
        COALESCE(
          (SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage', 'error_message')),
          CAST((SELECT value.int_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage', 'error_message')) AS STRING)
        ),
        ''
      ),
      '__NO_ERR_MSG__'
    ) AS failure_reason
  FROM `transferred.hudi_ods.screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'screw_puzzle'
   AND e.user_id = u.user_id
   AND DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') = u.event_date
   AND e.event_timestamp >= u.min_timestamp
  WHERE e.event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile')

  UNION ALL

  SELECT
    u.product,
    u.event_date,
    u.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    COALESCE(
      NULLIF((SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key = 'network_name'), ''),
      '__NO_NETWORK__'
    ) AS network_name,
    COALESCE(
      NULLIF(
        COALESCE(
          (SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage', 'error_message')),
          CAST((SELECT value.int_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage', 'error_message')) AS STRING)
        ),
        ''
      ),
      '__NO_ERR_MSG__'
    ) AS failure_reason
  FROM `transferred.hudi_ods.ios_screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'ios_screw_puzzle'
   AND e.user_id = u.user_id
   AND DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') = u.event_date
   AND e.event_timestamp >= u.min_timestamp
  WHERE e.event_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    AND e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile')
)
SELECT
  product,
  CAST(event_date AS STRING) AS event_date,
  ad_format,
  experiment_group,
  network_name,
  failure_reason,
  COUNT(*) AS reason_pv
FROM scoped_events
WHERE ad_format IS NOT NULL
GROUP BY product, event_date, ad_format, experiment_group, network_name, failure_reason
ORDER BY product, ad_format, network_name, failure_reason, event_date, experiment_group
