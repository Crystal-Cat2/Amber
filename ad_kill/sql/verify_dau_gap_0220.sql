SELECT
  product,
  ab_group,
  COUNT(DISTINCT user_pseudo_id) AS dau_all_events,
  COUNT(DISTINCT CASE WHEN event_name = 'user_engagement' THEN user_pseudo_id END) AS dau_engagement_only
FROM `commercial-adx.lmh.ad_kill_detail`
WHERE event_date = '2026-02-20'
GROUP BY product, ab_group
ORDER BY product, ab_group
