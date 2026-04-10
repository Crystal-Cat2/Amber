-- ad_kill 实验 AB 组 DAU 及新老用户构成分析
-- 数据源：commercial-adx.lmh.ad_kill_detail
-- 目的：确认 AB 两组用户量级和新老用户占比是否均衡
-- 输出：每日明细 + 周期内去重汇总（event_date = 'total'）

WITH daily_users AS (
  SELECT
    product,
    event_date,
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS dau
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'user_engagement'
  GROUP BY product, event_date, ab_group, user_type
),

daily_total AS (
  SELECT
    product,
    event_date,
    ab_group,
    SUM(dau) AS total_dau
  FROM daily_users
  GROUP BY product, event_date, ab_group
),

-- 周期内按 user_type 去重
period_users AS (
  SELECT
    product,
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS period_uv
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'user_engagement'
  GROUP BY product, ab_group, user_type
),

-- 周期内全量去重（不分 user_type）
period_total AS (
  SELECT
    product,
    ab_group,
    COUNT(DISTINCT user_pseudo_id) AS total_period_uv
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'user_engagement'
  GROUP BY product, ab_group
)

-- 每日明细
SELECT
  d.product,
  CAST(d.event_date AS STRING) AS event_date,
  d.ab_group,
  t.total_dau,
  d.user_type,
  d.dau AS type_dau,
  ROUND(SAFE_DIVIDE(d.dau, t.total_dau), 4) AS type_ratio
FROM daily_users d
JOIN daily_total t
  ON d.product = t.product
  AND d.event_date = t.event_date
  AND d.ab_group = t.ab_group

UNION ALL

-- 周期去重汇总
SELECT
  p.product,
  'total' AS event_date,
  p.ab_group,
  pt.total_period_uv AS total_dau,
  p.user_type,
  p.period_uv AS type_dau,
  ROUND(SAFE_DIVIDE(p.period_uv, pt.total_period_uv), 4) AS type_ratio
FROM period_users p
JOIN period_total pt
  ON p.product = pt.product
  AND p.ab_group = pt.ab_group

ORDER BY product, event_date, ab_group, user_type;
