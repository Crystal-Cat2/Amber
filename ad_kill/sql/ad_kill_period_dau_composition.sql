-- ad_kill 实验：周期级 DAU 新老用户构成
-- 数据源：transferred.hudi_ods.ball_sort / ios_nuts_sort
-- 目的：统计整个实验周期内（非分日）AB 组的 DAU 构成，区分新老用户
-- 输出列：product, ab_group, total_dau, user_type, type_dau, type_pv

-- ============================================================
-- Ball Sort: AB 分组（user_engagement 事件去重用户）
-- ============================================================
WITH bs_user_ab AS (
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

-- Ball Sort: 新用户（first_open 窗口内首次打开）
bs_new_users AS (
  SELECT
    fo.user_pseudo_id,
    MIN(DATE(TIMESTAMP_MICROS(fo.event_timestamp), 'UTC')) AS install_date,
    ua.ab_group
  FROM `transferred.hudi_ods.ball_sort` AS fo
  INNER JOIN bs_user_ab AS ua ON fo.user_pseudo_id = ua.user_pseudo_id
  WHERE fo.event_date BETWEEN '2026-01-29' AND '2026-03-08'
    AND fo.event_name = 'first_open'
  GROUP BY fo.user_pseudo_id, ua.ab_group
  HAVING install_date BETWEEN DATE '2026-01-30' AND DATE '2026-03-08'
),

-- Ball Sort: 全量 user_engagement 事件（用于计算 PV）
bs_engagement AS (
  SELECT
    e.user_pseudo_id,
    ua.ab_group,
    CASE WHEN nu.user_pseudo_id IS NOT NULL THEN 'new' ELSE 'old' END AS user_type
  FROM `transferred.hudi_ods.ball_sort` AS e
  INNER JOIN bs_user_ab AS ua ON e.user_pseudo_id = ua.user_pseudo_id
  LEFT JOIN bs_new_users AS nu
    ON e.user_pseudo_id = nu.user_pseudo_id AND ua.ab_group = nu.ab_group
  WHERE e.event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND e.event_name = 'user_engagement'
),

-- Ball Sort: 周期级 total_dau
bs_total_dau AS (
  SELECT
    ab_group,
    COUNT(DISTINCT user_pseudo_id) AS total_dau
  FROM bs_engagement
  GROUP BY ab_group
),

-- Ball Sort: 按 user_type 的 UV 和 PV
bs_type_stats AS (
  SELECT
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS type_dau,
    COUNT(*) AS type_pv
  FROM bs_engagement
  GROUP BY ab_group, user_type
),

-- ============================================================
-- iOS Nuts Sort: AB 分组
-- ============================================================
ns_user_ab AS (
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

-- iOS Nuts Sort: 新用户
ns_new_users AS (
  SELECT
    fo.user_pseudo_id,
    MIN(DATE(TIMESTAMP_MICROS(fo.event_timestamp), 'UTC')) AS install_date,
    ua.ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort` AS fo
  INNER JOIN ns_user_ab AS ua ON fo.user_pseudo_id = ua.user_pseudo_id
  WHERE fo.event_date BETWEEN '2026-02-01' AND '2026-03-08'
    AND fo.event_name = 'first_open'
  GROUP BY fo.user_pseudo_id, ua.ab_group
  HAVING install_date BETWEEN DATE '2026-02-02' AND DATE '2026-03-08'
),

-- iOS Nuts Sort: 全量 user_engagement 事件
ns_engagement AS (
  SELECT
    e.user_pseudo_id,
    ua.ab_group,
    CASE WHEN nu.user_pseudo_id IS NOT NULL THEN 'new' ELSE 'old' END AS user_type
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN ns_user_ab AS ua ON e.user_pseudo_id = ua.user_pseudo_id
  LEFT JOIN ns_new_users AS nu
    ON e.user_pseudo_id = nu.user_pseudo_id AND ua.ab_group = nu.ab_group
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name = 'user_engagement'
),

-- iOS Nuts Sort: 周期级 total_dau
ns_total_dau AS (
  SELECT
    ab_group,
    COUNT(DISTINCT user_pseudo_id) AS total_dau
  FROM ns_engagement
  GROUP BY ab_group
),

-- iOS Nuts Sort: 按 user_type 的 UV 和 PV
ns_type_stats AS (
  SELECT
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS type_dau,
    COUNT(*) AS type_pv
  FROM ns_engagement
  GROUP BY ab_group, user_type
)

-- 合并两个产品的结果
SELECT
  'ball_sort' AS product,
  s.ab_group,
  t.total_dau,
  s.user_type,
  s.type_dau,
  s.type_pv
FROM bs_type_stats AS s
JOIN bs_total_dau AS t ON s.ab_group = t.ab_group

UNION ALL

SELECT
  'ios_nuts_sort' AS product,
  s.ab_group,
  t.total_dau,
  s.user_type,
  s.type_dau,
  s.type_pv
FROM ns_type_stats AS s
JOIN ns_total_dau AS t ON s.ab_group = t.ab_group

ORDER BY product, ab_group, user_type;
