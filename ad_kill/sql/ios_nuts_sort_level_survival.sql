-- ad_kill 实验：iOS Nuts Sort 分关卡存活率
-- 逻辑：LT30 内新用户的 max(levelid)，max_level=N 表示在 1~N 关都存活
-- 横轴：关卡号，纵轴：到达该关卡的用户占比
-- 事件：game_new_start，activity_id=0（主线关卡）
-- 分组：game_model 2a(A) / 2b(B)

-- 1a. 从 user_engagement 获取用户的 game_model 分组
WITH user_ab AS (
  SELECT
    user_pseudo_id,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-02-01' AND '2026-04-07'
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
  FROM `transferred.hudi_ods.ios_nuts_sort` AS fo
  INNER JOIN user_ab AS ua ON fo.user_pseudo_id = ua.user_pseudo_id
  WHERE fo.event_date BETWEEN '2026-02-01' AND '2026-03-08'
    AND fo.event_name = 'first_open'
  GROUP BY fo.user_pseudo_id, ua.ab_group
  HAVING install_date BETWEEN DATE '2026-02-02' AND DATE '2026-03-08'
),

-- 2. 每用户在 LT0~LT30 内的最大关卡
user_max_level AS (
  SELECT
    nu.ab_group,
    e.user_pseudo_id,
    MAX(
      CAST((SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'levelid' LIMIT 1) AS INT64)
    ) AS max_level
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
    AND (SELECT ep.value.int_value FROM UNNEST(e.event_params.array) AS ep WHERE ep.key = 'activity_id' LIMIT 1) = 0
    AND DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) BETWEEN 0 AND 30
  GROUP BY nu.ab_group, e.user_pseudo_id
),

-- 3. 每组有游戏记录的用户总数
level_user_count AS (
  SELECT ab_group, COUNT(DISTINCT user_pseudo_id) AS total_users
  FROM user_max_level
  GROUP BY ab_group
),

-- 4. 生成关卡序列，计算存活率
level_survival AS (
  SELECT
    uml.ab_group,
    level,
    COUNT(DISTINCT CASE WHEN uml.max_level >= level THEN uml.user_pseudo_id END) AS survived_users
  FROM user_max_level AS uml
  CROSS JOIN UNNEST(GENERATE_ARRAY(1, 500)) AS level
  GROUP BY uml.ab_group, level
)

-- 最终输出
SELECT
  ls.ab_group,
  ls.level,
  ls.survived_users,
  luc.total_users,
  SAFE_DIVIDE(ls.survived_users, luc.total_users) AS survival_rate
FROM level_survival AS ls
JOIN level_user_count AS luc ON ls.ab_group = luc.ab_group
WHERE ls.survived_users > 0
ORDER BY ls.ab_group, ls.level;
