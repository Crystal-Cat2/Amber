-- 查看第5关用户中：有些事件有 game_model（AB 分组），有些事件没有的用户数
-- 即同一用户在第5关的不同事件上 game_model 时有时无

WITH user_events AS (
  SELECT
    user_pseudo_id,
    event_timestamp,
    (SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model') AS game_model,
    (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
),

user_summary AS (
  SELECT
    user_pseudo_id,
    COUNTIF(game_model IS NOT NULL) AS events_with_gm,
    COUNTIF(game_model IS NULL) AS events_without_gm,
    COUNTIF(ad_kill_scene IS NOT NULL) AS events_with_scene,
    COUNTIF(ad_kill_scene IS NULL) AS events_without_scene
  FROM user_events
  GROUP BY user_pseudo_id
)

SELECT
  CASE
    WHEN events_with_gm > 0 AND events_without_gm > 0 THEN 'mixed_gm'
    WHEN events_with_gm > 0 AND events_without_gm = 0 THEN 'all_has_gm'
    WHEN events_with_gm = 0 AND events_without_gm > 0 THEN 'all_no_gm'
  END AS gm_status,
  COUNT(*) AS users,
  SUM(CASE WHEN events_without_scene > 0 THEN 1 ELSE 0 END) AS users_with_null_scene
FROM user_summary
GROUP BY gm_status
ORDER BY users DESC