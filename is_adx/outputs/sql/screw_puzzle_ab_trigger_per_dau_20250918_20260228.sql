-- 查询目的：
-- 1. 基于 lib_isx_group 识别 screw_puzzle / ios_screw_puzzle 的 AB 分组用户。
-- 2. 保留用户在 UTC-0 当天最早分组事件之后的广告事件。
-- 3. 按实验周期、产品、广告位、实验组汇总 trigger 总次数与周期内每日 UV 加和。
-- 4. 输出周期级人均日均 trigger = trigger_cnt / dau_user_days。

WITH source_events AS (
  SELECT
    'screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS utc_event_date,
    event_timestamp,
    user_id,
    event_name,
    event_params.array AS event_params_array
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE (
      event_date BETWEEN '2025-09-17' AND '2025-12-19'
      OR event_date BETWEEN '2025-12-31' AND '2026-03-01'
    )
    AND event_name IN (
      'lib_isx_group',
      'interstitial_ad_request',
      'interstitial_ad_fill',
      'interstitial_ad_trigger',
      'interstitial_ad_show',
      'interstitial_ad_impression',
      'reward_ad_request',
      'reward_ad_fill',
      'reward_ad_trigger',
      'reward_ad_show',
      'reward_ad_impression'
    )

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS utc_event_date,
    event_timestamp,
    user_id,
    event_name,
    event_params.array AS event_params_array
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE (
      event_date BETWEEN '2025-09-17' AND '2025-12-19'
      OR event_date BETWEEN '2025-12-31' AND '2026-03-01'
    )
    AND event_name IN (
      'lib_isx_group',
      'interstitial_ad_request',
      'interstitial_ad_fill',
      'interstitial_ad_trigger',
      'interstitial_ad_show',
      'interstitial_ad_impression',
      'reward_ad_request',
      'reward_ad_fill',
      'reward_ad_trigger',
      'reward_ad_show',
      'reward_ad_impression'
    )
),

-- 按用户 + UTC 日 + 实验组取当天最早进组时间，后续广告事件仅保留该时间之后的数据。
experiment_user_daily_range AS (
  SELECT
    product,
    utc_event_date,
    user_id,
    CASE
      WHEN (
        SELECT value.string_value
        FROM UNNEST(event_params_array)
        WHERE key = 'group'
      ) = 'A' THEN 'no_is_adx'
      WHEN (
        SELECT value.string_value
        FROM UNNEST(event_params_array)
        WHERE key = 'group'
      ) = 'B' THEN 'have_is_adx'
      ELSE NULL
    END AS experiment_group,
    MIN(event_timestamp) AS min_timestamp
  FROM source_events
  WHERE event_name = 'lib_isx_group'
    AND user_id IS NOT NULL
    AND (
      utc_event_date BETWEEN DATE '2025-09-18' AND DATE '2025-12-18'
      OR utc_event_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28'
    )
  GROUP BY 1, 2, 3, 4
  HAVING experiment_group IS NOT NULL
),

-- 回连同一用户同一天、且发生在最早分组事件之后的广告事件。
scoped_ad_events AS (
  SELECT
    r.product,
    r.utc_event_date,
    r.user_id,
    r.experiment_group,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      ELSE NULL
    END AS ad_format,
    CASE
      WHEN r.utc_event_date BETWEEN DATE '2025-09-18' AND DATE '2025-12-18' THEN '0918-1218'
      WHEN r.utc_event_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28' THEN '0101-0228'
      ELSE NULL
    END AS period_label,
    e.event_name
  FROM source_events AS e
  JOIN experiment_user_daily_range AS r
    ON e.product = r.product
   AND e.user_id = r.user_id
   AND e.utc_event_date = r.utc_event_date
   AND e.event_timestamp >= r.min_timestamp
  WHERE e.event_name IN (
      'interstitial_ad_request',
      'interstitial_ad_fill',
      'interstitial_ad_trigger',
      'interstitial_ad_show',
      'interstitial_ad_impression',
      'reward_ad_request',
      'reward_ad_fill',
      'reward_ad_trigger',
      'reward_ad_show',
      'reward_ad_impression'
    )
),

period_trigger_summary AS (
  SELECT
    product,
    period_label,
    ad_format,
    experiment_group,
    COUNTIF(event_name IN ('interstitial_ad_trigger', 'reward_ad_trigger')) AS trigger_cnt
  FROM scoped_ad_events
  WHERE period_label IS NOT NULL
    AND ad_format IS NOT NULL
  GROUP BY 1, 2, 3, 4
),

period_dau_summary AS (
  SELECT
    product,
    experiment_group,
    CASE
      WHEN utc_event_date BETWEEN DATE '2025-09-18' AND DATE '2025-12-18' THEN '0918-1218'
      WHEN utc_event_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28' THEN '0101-0228'
      ELSE NULL
    END AS period_label,
    COUNT(*) AS dau_user_days
  FROM experiment_user_daily_range
  WHERE (
      utc_event_date BETWEEN DATE '2025-09-18' AND DATE '2025-12-18'
      OR utc_event_date BETWEEN DATE '2026-01-01' AND DATE '2026-02-28'
    )
  GROUP BY 1, 2, 3
)

SELECT
  t.product,
  t.period_label,
  t.ad_format,
  t.experiment_group,
  t.trigger_cnt,
  d.dau_user_days,
  ROUND(SAFE_DIVIDE(t.trigger_cnt, d.dau_user_days), 4) AS avg_trigger_per_dau_day
FROM period_trigger_summary AS t
LEFT JOIN period_dau_summary AS d
  ON t.product = d.product
 AND t.period_label = d.period_label
 AND t.experiment_group = d.experiment_group
ORDER BY
  CASE
    WHEN period_label = '0918-1218' THEN 1
    WHEN period_label = '0101-0228' THEN 2
    ELSE 99
  END,
  CASE
    WHEN product = 'screw_puzzle' THEN 1
    WHEN product = 'ios_screw_puzzle' THEN 2
    ELSE 99
  END,
  CASE
    WHEN ad_format = 'interstitial' THEN 1
    WHEN ad_format = 'rewarded' THEN 2
    ELSE 99
  END,
  CASE
    WHEN experiment_group = 'no_is_adx' THEN 1
    WHEN experiment_group = 'have_is_adx' THEN 2
    ELSE 99
  END;
