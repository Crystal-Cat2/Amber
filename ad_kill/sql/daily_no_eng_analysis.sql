-- 分日无 engagement 用户分析：新老占比 + 事件分布
-- 数据源：commercial-adx.lmh.ad_kill_detail
WITH eng_users AS (
  SELECT DISTINCT product, event_date, user_pseudo_id
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'user_engagement'
),

no_eng AS (
  SELECT d.product, d.event_date, d.user_pseudo_id, d.user_type, d.event_name
  FROM `commercial-adx.lmh.ad_kill_detail` d
  LEFT JOIN eng_users e
    ON d.product = e.product
   AND d.event_date = e.event_date
   AND d.user_pseudo_id = e.user_pseudo_id
  WHERE e.user_pseudo_id IS NULL
)

SELECT product, event_date, 'user_type' AS dim, user_type AS val,
  COUNT(DISTINCT user_pseudo_id) AS cnt
FROM no_eng
GROUP BY product, event_date, user_type

UNION ALL

SELECT product, event_date, 'event' AS dim, event_name AS val,
  COUNT(DISTINCT user_pseudo_id) AS cnt
FROM no_eng
GROUP BY product, event_date, event_name
