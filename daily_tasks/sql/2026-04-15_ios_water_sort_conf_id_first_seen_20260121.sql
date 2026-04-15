DECLARE event_window_start STRING DEFAULT '2026-01-18';
DECLARE event_window_end STRING DEFAULT '2026-01-25';
DECLARE cutoff_date STRING DEFAULT '2026-01-21';

-- 目标：
-- 1. 只看 ios_water_sort 中广告事件（inter / reward / banner）。
-- 2. 在 ad_events 中限制事件范围到 2026-01-18 ~ 2026-01-25。
-- 3. 找出在该窗口内于 2026-01-21 之后首次出现、且在 2026-01-18 ~ 2026-01-20 之间未出现过的 conf_id。
-- 4. 为后续替换实验 conf_id 集合提供候选名单。

WITH ad_events AS (
  SELECT
    event_date,
    (
      SELECT event_param.value.string_value
      FROM UNNEST(event_params.array) AS event_param
      WHERE event_param.key = 'conf_id'
      LIMIT 1
    ) AS conf_id,
    CASE
      WHEN event_name LIKE 'inter%' THEN 'INTER'
      WHEN event_name LIKE 'reward%' THEN 'REWARD'
      WHEN event_name LIKE 'banner%' THEN 'BANNER'
      ELSE NULL
    END AS ad_format
  FROM `transferred.hudi_ods.ios_water_sort`
  WHERE event_date IS NOT NULL
    AND event_date BETWEEN event_window_start AND event_window_end
    AND (
      event_name LIKE 'inter%'
      OR event_name LIKE 'reward%'
      OR event_name LIKE 'banner%'
    )
),

conf_id_first_seen AS (
  SELECT
    conf_id,
    ARRAY_AGG(DISTINCT ad_format IGNORE NULLS ORDER BY ad_format) AS ad_formats,
    MIN(event_date) AS first_seen_date,
    MAX(IF(event_date < cutoff_date, 1, 0)) AS seen_before_cutoff,
    MAX(IF(event_date >= cutoff_date, 1, 0)) AS seen_on_or_after_cutoff
  FROM ad_events
  WHERE conf_id IS NOT NULL
    AND conf_id != ''
    AND ad_format IS NOT NULL
  GROUP BY 1
)

SELECT
  conf_id,
  ad_formats,
  first_seen_date
FROM conf_id_first_seen
WHERE seen_before_cutoff = 0
  AND seen_on_or_after_cutoff = 1
  AND first_seen_date BETWEEN cutoff_date AND event_window_end
ORDER BY first_seen_date, conf_id;
