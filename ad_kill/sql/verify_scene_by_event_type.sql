-- 有 AB 分组的用户，分 event_name 看 ad_kill_scene 分布
SELECT
  event_name,
  (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS scene,
  COUNT(*) AS events,
  COUNT(DISTINCT user_pseudo_id) AS users
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name IN ('game_new_start', 'game_win')
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  AND REGEXP_CONTAINS(
    LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
    r'(^|[^a-z0-9])(12a|12b)([^a-z0-9]|$)'
  )
GROUP BY event_name, scene
ORDER BY event_name, users DESC