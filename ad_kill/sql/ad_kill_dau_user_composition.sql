-- ad_kill 实验 AB 组 DAU 及新老用户构成分析
-- 数据源：commercial-adx.lmh.ad_kill_detail
-- 目的：确认 AB 两组用户量级和新老用户占比是否均衡

WITH daily_users AS (
  SELECT
    product,
    event_date,
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS dau
  FROM `commercial-adx.lmh.ad_kill_detail`
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
)

SELECT
  d.product,
  d.event_date,
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
ORDER BY d.product, d.event_date, d.ab_group, d.user_type;
