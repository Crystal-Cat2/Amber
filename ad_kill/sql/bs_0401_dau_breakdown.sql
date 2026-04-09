-- 安卓 ball_sort 0401: 全事件DAU / engagement DAU / 无engagement DAU / 无engagement有alive DAU
WITH base AS (
  SELECT
    user_pseudo_id,
    MAX(CASE WHEN event_name = 'user_engagement' THEN 1 ELSE 0 END) AS has_engagement,
    MAX(CASE WHEN event_name = 'universal_alive' THEN 1 ELSE 0 END) AS has_alive,
    MAX(CASE WHEN event_name LIKE 'game%' THEN 1 ELSE 0 END) AS has_game
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date = '2026-04-01'
  GROUP BY user_pseudo_id
)
SELECT
  COUNT(*) AS dau_all,
  SUM(has_engagement) AS dau_engagement,
  SUM(CASE WHEN has_engagement = 0 THEN 1 ELSE 0 END) AS dau_no_engagement,
  SUM(CASE WHEN has_engagement = 0 AND has_game = 0 AND has_alive = 1 THEN 1 ELSE 0 END) AS dau_no_eng_no_game_has_alive
FROM base
