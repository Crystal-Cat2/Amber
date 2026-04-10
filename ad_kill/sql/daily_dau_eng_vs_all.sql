-- 分日口径对齐：全事件 DAU vs 仅 user_engagement DAU
-- 数据源：commercial-adx.lmh.ad_kill_detail
SELECT
  product,
  event_date,
  ab_group,
  COUNT(DISTINCT user_pseudo_id) AS dau_all,
  COUNT(DISTINCT CASE WHEN event_name = 'user_engagement' THEN user_pseudo_id END) AS dau_eng
FROM `commercial-adx.lmh.ad_kill_detail`
GROUP BY product, event_date, ab_group
ORDER BY product, event_date, ab_group
