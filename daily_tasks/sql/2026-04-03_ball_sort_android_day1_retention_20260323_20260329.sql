-- 倒球安卓上周 3 日留存（日维度）
-- 逻辑：
-- 1. 用 first_open 定义 2026-03-23 到 2026-03-29 的新增用户。
-- 2. 用 user_engagement 判断这些用户是否在安装后第 3 天回访。
-- 3. 按安装日输出新增、3 日留存人数和 3 日留存率。
WITH new_users AS (
  -- 提取上周安卓新增用户，并按 UTC 落安装日
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS install_date,
    user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-03-23' AND '2026-03-29'
    AND event_name = 'first_open'
    AND platform = 'ANDROID'
  GROUP BY
    install_date,
    user_pseudo_id
),
day3_active AS (
  -- 提取安装后第 3 天有活跃事件的用户，并回连到安装日
  SELECT
    new_users.install_date,
    new_users.user_pseudo_id
  FROM new_users
  INNER JOIN `transferred.hudi_ods.ball_sort` AS active_events
    ON active_events.user_pseudo_id = new_users.user_pseudo_id
   AND DATE(TIMESTAMP_MICROS(active_events.event_timestamp), 'UTC') = DATE_ADD(new_users.install_date, INTERVAL 3 DAY)
  WHERE active_events.event_date BETWEEN '2026-03-26' AND '2026-04-01'
    AND active_events.event_name = 'user_engagement'
    AND active_events.platform = 'ANDROID'
  GROUP BY
    new_users.install_date,
    new_users.user_pseudo_id
)
SELECT
  new_users.install_date,
  COUNT(DISTINCT new_users.user_pseudo_id) AS new_users,
  COUNT(DISTINCT day3_active.user_pseudo_id) AS day3_retained_users,
  SAFE_DIVIDE(
    COUNT(DISTINCT day3_active.user_pseudo_id),
    COUNT(DISTINCT new_users.user_pseudo_id)
  ) AS day3_retention_rate
FROM new_users
LEFT JOIN day3_active
  ON day3_active.install_date = new_users.install_date
 AND day3_active.user_pseudo_id = new_users.user_pseudo_id
GROUP BY
  new_users.install_date
ORDER BY
  new_users.install_date;
