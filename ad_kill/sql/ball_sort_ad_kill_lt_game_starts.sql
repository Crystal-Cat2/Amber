-- ad_kill 实验：Ball Sort Android 新用户 LT 留存 + LT 内累计人均开局次数
-- 横轴：LT day (LT0 ~ LT30)，累计值不因中间天无数据而断档
-- 新用户：first_open 在 1/30 ~ 3/8
-- 分组：game_model 12a(A) / 12b(B)
-- 开局事件：game_new_start

-- 1a. 从 user_engagement 获取用户的 game_model 分组
WITH user_ab AS (
  SELECT
    user_pseudo_id,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name = 'user_engagement'
    AND (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
  HAVING ab_group IS NOT NULL
),

-- 1b. 用 first_open 确定 install_date
new_users AS (
  SELECT
    fo.user_pseudo_id,
    MIN(DATE(TIMESTAMP_MICROS(fo.event_timestamp), 'UTC')) AS install_date,
    ua.ab_group
  FROM `transferred.hudi_ods.ball_sort` AS fo
  INNER JOIN user_ab AS ua ON fo.user_pseudo_id = ua.user_pseudo_id
  WHERE fo.event_date BETWEEN '2026-01-29' AND '2026-03-08'
    AND fo.event_name = 'first_open'
  GROUP BY fo.user_pseudo_id, ua.ab_group
  HAVING install_date BETWEEN DATE '2026-01-30' AND DATE '2026-03-08'
),

new_user_count AS (
  SELECT ab_group, COUNT(DISTINCT user_pseudo_id) AS total_new_users
  FROM new_users
  GROUP BY ab_group
),
