-- 查询目的：按产品、AB 组、广告格式与渠道输出 display_failed 的错误信息分布。
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
display_failed_events AS (
  SELECT
    u.product,
    u.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(NULLIF((SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key = 'network_name'), ''), '__NO_NETWORK__') AS network_name,
    COALESCE(
      NULLIF(
        COALESCE(
          (SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage')),
          CAST((SELECT value.int_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage')) AS STRING)
        ),
        ''
      ),
      '__NO_ERR_MSG__'
    ) AS failure_reason
  FROM `transferred.hudi_ods.screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'screw_puzzle'
   AND e.user_id = u.user_id
   AND e.event_timestamp >= u.min_timestamp
   AND e.event_timestamp <= u.max_timestamp
  WHERE e.event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile')

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
    COALESCE(
      NULLIF(
        COALESCE(
          (SELECT value.string_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage')),
          CAST((SELECT value.int_value FROM UNNEST(e.event_params.array) WHERE key IN ('err_msg', 'error_massage')) AS STRING)
        ),
        ''
      ),
      '__NO_ERR_MSG__'
    ) AS failure_reason
  FROM `transferred.hudi_ods.ios_screw_puzzle` AS e
  JOIN experiment_users AS u
    ON u.product = 'ios_screw_puzzle'
   AND e.user_id = u.user_id
   AND e.event_timestamp >= u.min_timestamp
   AND e.event_timestamp <= u.max_timestamp
  WHERE e.event_date BETWEEN '2025-09-18' AND '2026-01-03'
    AND e.event_name IN ('interstitial_ad_display_failed', 'reward_ad_display_faile')
),
reason_counts AS (
  SELECT
    product,
    experiment_group,
    ad_format,
    network_name,
    failure_reason,
    COUNT(*) AS reason_pv
  FROM display_failed_events
  GROUP BY product, experiment_group, ad_format, network_name, failure_reason
),
network_totals AS (
  SELECT
    product,
    experiment_group,
    ad_format,
    network_name,
    SUM(reason_pv) AS display_failed_pv
  FROM reason_counts
  GROUP BY product, experiment_group, ad_format, network_name
)
SELECT
  r.product,
  r.experiment_group,
  r.ad_format,
  r.network_name,
  r.failure_reason,
  n.display_failed_pv,
  r.reason_pv,
  SAFE_DIVIDE(r.reason_pv, n.display_failed_pv) AS reason_share_in_network
FROM reason_counts AS r
JOIN network_totals AS n
  ON r.product = n.product
 AND r.experiment_group = n.experiment_group
 AND r.ad_format = n.ad_format
 AND r.network_name = n.network_name
ORDER BY
  r.product,
  r.ad_format,
  r.experiment_group,
  n.display_failed_pv DESC,
  r.network_name,
  r.reason_pv DESC,
  r.failure_reason;
