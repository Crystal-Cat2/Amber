-- 最后一条 game_new_start scene=none 的用户，第5关杀广告事件 PV 分布
WITH last_gns AS (
  SELECT user_pseudo_id
  FROM (
    SELECT
      user_pseudo_id,
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene,
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
  WHERE rn = 1 AND ad_kill_scene = 'none'
),

kill_pv AS (
  SELECT lg.user_pseudo_id, COUNT(e.event_name) AS kill_count
  FROM last_gns lg
  LEFT JOIN (
    SELECT user_pseudo_id, event_name
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
      AND event_name = 'lib_fullscreen_ad_killed'
      AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  ) e ON lg.user_pseudo_id = e.user_pseudo_id
  GROUP BY lg.user_pseudo_id
)

SELECT
  kill_count,
  COUNT(*) AS users,
  ROUND(SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 4) AS ratio
FROM kill_pv
GROUP BY kill_count
ORDER BY kill_count
LIMIT 20