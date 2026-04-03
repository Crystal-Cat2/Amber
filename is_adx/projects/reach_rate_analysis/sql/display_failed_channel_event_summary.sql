-- 查询目的：按产品、AB 组、广告格式与渠道输出 trigger / show / impression / display_failed 的 PV 与漏斗指标。
WITH experiment_users AS (
  SELECT
    'screw_puzzle' AS product,
    user_id,
    CASE (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group')
      WHEN 'A' THEN 'no_is_adx'
      WHEN 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp,
    MAX(event_timestamp) AS max_timestamp
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND event_name = 'lib_isx_group'
    AND user_id IS NOT NULL
  GROUP BY 1, 2, 3
  HAVING experiment_group IS NOT NULL

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    user_id,
    CASE (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'group')
      WHEN 'A' THEN 'no_is_adx'
      WHEN 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp,
    MAX(event_timestamp) AS max_timestamp
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND event_name = 'lib_isx_group'
    AND user_id IS NOT NULL
  GROUP BY 1, 2, 3
  HAVING experiment_group IS NOT NULL
),
scoped_events AS (
  SELECT
    u.product,
    u.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(NULLIF((SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key = 'network_name'), ''), '__NO_NETWORK__') AS network_name,
    CASE
      WHEN e.event_name IN ('interstitial_ad_trigger', 'reward_ad_trigger') THEN 'trigger'
      WHEN e.event_name IN ('interstitial_ad_show', 'reward_ad_show') THEN 'show'
      WHEN e.event_name IN ('interstitial_ad_impression', 'reward_ad_impression') THEN 'impression'
      WHEN e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile') THEN 'display_failed'
      ELSE NULL
    END AS event_type
  FROM `transferred.hudi_ods.screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'screw_puzzle'
   AND e.user_id = u.user_id
   AND e.event_timestamp >= u.min_timestamp
   AND e.event_timestamp <= u.max_timestamp
  WHERE e.event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND e.event_name IN (
      'interstitial_ad_trigger', 'reward_ad_trigger',
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression',
      'interstitial_ad_display_failed', 'reward_ad_display_faile'
    )

  UNION ALL

  SELECT
    u.product,
    u.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(NULLIF((SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key = 'network_name'), ''), '__NO_NETWORK__') AS network_name,
    CASE
      WHEN e.event_name IN ('interstitial_ad_trigger', 'reward_ad_trigger') THEN 'trigger'
      WHEN e.event_name IN ('interstitial_ad_show', 'reward_ad_show') THEN 'show'
      WHEN e.event_name IN ('interstitial_ad_impression', 'reward_ad_impression') THEN 'impression'
      WHEN e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile') THEN 'display_failed'
      ELSE NULL
    END AS event_type
  FROM `transferred.hudi_ods.ios_screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'ios_screw_puzzle'
   AND e.user_id = u.user_id
   AND e.event_timestamp >= u.min_timestamp
   AND e.event_timestamp <= u.max_timestamp
  WHERE e.event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND e.event_name IN (
      'interstitial_ad_trigger', 'reward_ad_trigger',
      'interstitial_ad_show', 'reward_ad_show',
      'interstitial_ad_impression', 'reward_ad_impression',
      'interstitial_ad_display_failed', 'reward_ad_display_faile'
    )
),
network_event_counts AS (
  SELECT
    product,
    experiment_group,
    ad_format,
    network_name,
    COUNTIF(event_type = 'trigger') AS trigger_pv,
    COUNTIF(event_type = 'show') AS show_pv,
    COUNTIF(event_type = 'impression') AS impression_pv,
    COUNTIF(event_type = 'display_failed') AS display_failed_pv
  FROM scoped_events
  GROUP BY product, experiment_group, ad_format, network_name
),
display_failed_totals AS (
  SELECT
    product,
    experiment_group,
    ad_format,
    SUM(display_failed_pv) AS display_failed_total_pv
  FROM network_event_counts
  GROUP BY product, experiment_group, ad_format
)
SELECT
  c.product,
  c.experiment_group,
  c.ad_format,
  c.network_name,
  c.trigger_pv,
  c.show_pv,
  c.impression_pv,
  c.display_failed_pv,
  SAFE_DIVIDE(c.show_pv, c.trigger_pv) AS show_rate,
  SAFE_DIVIDE(c.impression_pv, c.show_pv) AS impression_rate,
  t.display_failed_total_pv,
  SAFE_DIVIDE(c.display_failed_pv, t.display_failed_total_pv) AS display_failed_share
FROM network_event_counts AS c
JOIN display_failed_totals AS t
  ON c.product = t.product
 AND c.experiment_group = t.experiment_group
 AND c.ad_format = t.ad_format
ORDER BY
  c.product,
  c.ad_format,
  c.experiment_group,
  c.display_failed_pv DESC,
  c.trigger_pv DESC,
  c.show_pv DESC,
  c.impression_pv DESC,
  c.network_name;
