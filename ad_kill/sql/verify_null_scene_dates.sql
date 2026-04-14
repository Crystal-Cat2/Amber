-- 查看 ad_kill_scene=NULL 事件的日期分布
SELECT
  event_date,
  COUNT(*) AS null_scene_events,
  COUNT(DISTINCT user_pseudo_id) AS null_scene_users
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name IN ('game_new_start', 'game_win')
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') IS NULL
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
GROUP BY event_date
ORDER BY event_date