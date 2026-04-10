-- 安卓 ball_sort + iOS nuts_sort 0401: DAU 拆解 + 无eng无game有alive的新老用户
WITH base AS (
  SELECT
    'ball_sort' AS product,
    user_pseudo_id,
    MAX(CASE WHEN event_name = 'user_engagement' THEN 1 ELSE 0 END) AS has_engagement,
    MAX(CASE WHEN event_name = 'universal_alive' THEN 1 ELSE 0 END) AS has_alive,
    MAX(CASE WHEN event_name LIKE 'game%' THEN 1 ELSE 0 END) AS has_game,
    MAX(CASE WHEN event_name = 'first_open' THEN 1 ELSE 0 END) AS has_first_open
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date = '2026-04-01'
  GROUP BY user_pseudo_id

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    user_pseudo_id,
    MAX(CASE WHEN event_name = 'user_engagement' THEN 1 ELSE 0 END) AS has_engagement,
    MAX(CASE WHEN event_name = 'universal_alive' THEN 1 ELSE 0 END) AS has_alive,
    MAX(CASE WHEN event_name LIKE 'game%' THEN 1 ELSE 0 END) AS has_game,
    MAX(CASE WHEN event_name = 'first_open' THEN 1 ELSE 0 END) AS has_first_open
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date = '2026-04-01'
  GROUP BY user_pseudo_id
)
SELECT
  product,
  COUNT(*) AS dau_all,
  SUM(has_engagement) AS dau_engagement,
  SUM(CASE WHEN has_engagement = 0 THEN 1 ELSE 0 END) AS dau_no_engagement,
  SUM(CASE WHEN has_engagement = 0 AND has_game = 0 AND has_alive = 1 THEN 1 ELSE 0 END) AS dau_no_eng_no_game_has_alive,
  SUM(CASE WHEN has_engagement = 0 AND has_game = 0 AND has_alive = 1 AND has_first_open = 1 THEN 1 ELSE 0 END) AS alive_only_new,
  SUM(CASE WHEN has_engagement = 0 AND has_game = 0 AND has_alive = 1 AND has_first_open = 0 THEN 1 ELSE 0 END) AS alive_only_old
FROM base
GROUP BY product
ORDER BY product
