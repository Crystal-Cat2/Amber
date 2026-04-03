-- ball_sort 上周次留数据
WITH new_users AS (
  SELECT
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS install_date,
    user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-03-24' AND '2026-03-30'
    AND event_name = 'first_open'
  GROUP BY install_date, user_pseudo_id
),
day1_active AS (
  SELECT
    nu.install_date,
    nu.user_pseudo_id
  FROM new_users nu
  INNER JOIN `transferred.hudi_ods.ball_sort` e
    ON e.user_pseudo_id = nu.user_pseudo_id
   AND DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') = DATE_ADD(nu.install_date, INTERVAL 1 DAY)
  WHERE e.event_date BETWEEN '2026-03-25' AND '2026-03-31'
  GROUP BY nu.install_date, nu.user_pseudo_id
)
SELECT
  nu.install_date,
  COUNT(DISTINCT nu.user_pseudo_id) AS new_users,
  COUNT(DISTINCT d1.user_pseudo_id) AS day1_retained_users,
  SAFE_DIVIDE(COUNT(DISTINCT d1.user_pseudo_id), COUNT(DISTINCT nu.user_pseudo_id)) AS day1_retention_rate
FROM new_users nu
LEFT JOIN day1_active d1
  ON nu.install_date = d1.install_date
 AND nu.user_pseudo_id = d1.user_pseudo_id
GROUP BY nu.install_date
ORDER BY nu.install_date
