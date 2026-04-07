-- ad_kill 实验 DAU 趋势
-- 目的：按天查看两个产品各实验组的 DAU，确定实验结束时间（DAU 明显下降点）
-- 分组来源：user_properties.game_model
-- Ball Sort Android: 12a(A) / 12b(B)
-- iOS Nuts Sort: 2a(A) / 2b(B)

WITH ball_sort_dau AS (
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
      ELSE NULL
    END AS ab_group,
    user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'user_engagement'
),
ios_nuts_sort_dau AS (
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
      ELSE NULL
    END AS ab_group,
    user_pseudo_id
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'user_engagement'
),
combined AS (
  SELECT * FROM ball_sort_dau
  UNION ALL
  SELECT * FROM ios_nuts_sort_dau
)
SELECT
  product,
  event_date,
  ab_group,
  COUNT(DISTINCT user_pseudo_id) AS dau
FROM combined
WHERE ab_group IS NOT NULL
GROUP BY product, event_date, ab_group
ORDER BY product, event_date, ab_group;
