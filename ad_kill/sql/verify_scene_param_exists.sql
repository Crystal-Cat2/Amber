-- 区分：ad_kill_scene 参数不存在 vs 参数存在但值为 NULL vs 有值
-- 只看有 AB 分组的 game_new_start 事件
SELECT
  CASE
    WHEN NOT EXISTS(SELECT 1 FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene')
      THEN 'no_param'
    WHEN (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') IS NULL
      THEN 'param_null_value'
    ELSE 'has_value'
  END AS param_status,
  (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS scene_value,
  COUNT(*) AS events,
  COUNT(DISTINCT user_pseudo_id) AS users
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name = 'game_new_start'
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  AND REGEXP_CONTAINS(
    LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
    r'(^|[^a-z0-9])(12a|12b)([^a-z0-9]|$)'
  )
GROUP BY param_status, scene_value
ORDER BY users DESC