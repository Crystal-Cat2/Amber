-- 验证：第5关事件中 ad_kill_scene 各值的分布
-- 检查 no_scene 为什么几乎消失了

SELECT
  'ball_sort' AS product,
  (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene') AS raw_scene,
  COALESCE(
    (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
    'NULL_coalesced'
  ) AS coalesced_scene,
  COUNT(*) AS event_count,
  COUNT(DISTINCT user_pseudo_id) AS user_count
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name IN ('game_new_start', 'game_win')
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
GROUP BY raw_scene, coalesced_scene
ORDER BY user_count DESC