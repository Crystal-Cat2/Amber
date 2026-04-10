-- ad_kill 实验：全部用户分日人均开局次数
-- 分日统计 game_new_start 总次数 / 当日 DAU（user_engagement 去重用户数）
-- Ball Sort Android: 12a(A) / 12b(B)
-- iOS Nuts Sort: 2a(A) / 2b(B)

WITH ball_sort_events AS (
  SELECT
    'ball_sort' AS product,
    event_date,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12b($|_)'
      ) THEN 'B'
    END AS ab_group,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('user_engagement', 'game_new_start')
),
ios_nuts_sort_events AS (
  SELECT
    'ios_nuts_sort' AS product,
    event_date,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2b($|_)'
      ) THEN 'B'
    END AS ab_group,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('user_engagement', 'game_new_start')
),
combined AS (
  SELECT * FROM ball_sort_events
  UNION ALL
  SELECT * FROM ios_nuts_sort_events
),
-- DAU：当日活跃用户数
daily_dau AS (
  SELECT
    product, event_date, ab_group,
    COUNT(DISTINCT user_pseudo_id) AS dau
  FROM combined
  WHERE ab_group IS NOT NULL
    AND event_name = 'user_engagement'
  GROUP BY product, event_date, ab_group
),
-- 开局次数
daily_starts AS (
  SELECT
    product, event_date, ab_group,
    COUNT(*) AS game_starts
  FROM combined
  WHERE ab_group IS NOT NULL
    AND event_name = 'game_new_start'
  GROUP BY product, event_date, ab_group
)

SELECT
  d.product,
  d.event_date,
  d.ab_group,
  d.dau,
  COALESCE(s.game_starts, 0) AS game_starts,
  SAFE_DIVIDE(COALESCE(s.game_starts, 0), d.dau) AS avg_game_starts_per_user
FROM daily_dau AS d
LEFT JOIN daily_starts AS s
  ON d.product = s.product AND d.event_date = s.event_date AND d.ab_group = s.ab_group
ORDER BY d.product, d.event_date, d.ab_group;