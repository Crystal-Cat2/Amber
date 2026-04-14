-- 验证：ad_kill_scene=NULL 事件中 ab_group 为 NULL 的比例
SELECT
  CASE WHEN
    REGEXP_CONTAINS(
      LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
      r'(^|[^a-z0-9])(12a|12b)([^a-z0-9]|$)'
    ) THEN 'has_ab'
    ELSE 'no_ab'
  END AS ab_status,
  COUNT(*) AS events,
  COUNT(DISTINCT user_pseudo_id) AS users
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name IN ('game_new_start', 'game_win')
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') IS NULL
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
GROUP BY ab_status