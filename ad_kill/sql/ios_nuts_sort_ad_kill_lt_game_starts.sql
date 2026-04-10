-- ad_kill 实验：iOS Nuts Sort 新用户 LT 留存 + LT 内累计人均开局次数
-- 横轴：LT day (LT0 ~ LT30)，累计值不因中间天无数据而断档
-- 新用户：first_open 在 2/2 ~ 3/8
-- 分组：game_model 2a(A) / 2b(B)
-- 开局事件：game_new_start

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

new_user_count AS (
  SELECT ab_group, COUNT(DISTINCT user_pseudo_id) AS total_new_users
  FROM new_users
  GROUP BY ab_group
),

-- 2. LT 留存
lt_raw AS (
  SELECT
    nu.ab_group,
    e.user_pseudo_id,
    DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) AS lt_day
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name = 'user_engagement'
),
lt_summary AS (
  SELECT ab_group, lt_day, COUNT(DISTINCT user_pseudo_id) AS retained_users
  FROM lt_raw
  WHERE lt_day BETWEEN 0 AND 30
  GROUP BY ab_group, lt_day
),

-- 3. 开局次数：game_new_start 按 LT day 聚合
starts_raw AS (
  SELECT
    nu.ab_group,
    DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) AS lt_day,
    COUNT(*) AS start_count
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name = 'game_new_start'
  GROUP BY ab_group, lt_day
),

-- 4. skeleton 防断档
ab_groups AS (SELECT 'A' AS ab_group UNION ALL SELECT 'B'),
lt_days AS (SELECT lt_day FROM UNNEST(GENERATE_ARRAY(0, 30)) AS lt_day),
skeleton AS (
  SELECT a.ab_group, d.lt_day
  FROM ab_groups a CROSS JOIN lt_days d
),

-- 5. 合并 + 累计
merged AS (
  SELECT
    s.ab_group,
    s.lt_day,
    COALESCE(sr.start_count, 0) AS start_count
  FROM skeleton AS s
  LEFT JOIN starts_raw AS sr
    ON s.ab_group = sr.ab_group AND s.lt_day = sr.lt_day
),
cumulative AS (
  SELECT
    ab_group, lt_day,
    SUM(start_count) OVER (PARTITION BY ab_group ORDER BY lt_day) AS cum_starts
  FROM merged
)

-- 最终输出
SELECT
  c.ab_group,
  c.lt_day,
  lt.retained_users,
  nuc.total_new_users,
  SAFE_DIVIDE(lt.retained_users, nuc.total_new_users) AS retention_rate,
  SAFE_DIVIDE(c.cum_starts, nuc.total_new_users) AS avg_cum_game_starts
FROM cumulative AS c
LEFT JOIN lt_summary AS lt
  ON c.ab_group = lt.ab_group AND c.lt_day = lt.lt_day
LEFT JOIN new_user_count AS nuc
  ON c.ab_group = nuc.ab_group
ORDER BY c.ab_group, c.lt_day;
