-- 取每个有 AB 分组用户的最后一条 game_new_start 事件
-- 看 ad_kill_scene 参数是否存在及各值 UV
WITH last_gns AS (
  SELECT user_pseudo_id, ad_kill_scene, param_exists
  FROM (
    SELECT
      user_pseudo_id,
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene,
      EXISTS(SELECT 1 FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS param_exists,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC) AS rn
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
  )
  WHERE rn = 1
)

SELECT
  CASE WHEN NOT param_exists THEN 'no_param'
       WHEN ad_kill_scene IS NULL THEN 'param_null'
       ELSE ad_kill_scene
  END AS scene,
  COUNT(*) AS users
FROM last_gns
GROUP BY scene
ORDER BY users DESC