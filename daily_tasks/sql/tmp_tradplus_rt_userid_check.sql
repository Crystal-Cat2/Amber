SELECT
  CASE
    WHEN log.app_id = 'BEC3799A1CB6EE4D97BF451CB4EB8408' THEN 'ios_car_jam'
    WHEN log.app_id = '4925CFAA3FA0144A611342E2076552BA' THEN 'car_jam'
  END AS product,
  COUNT(*) AS total_rows,
  COUNTIF(log.user_id IS NOT NULL AND log.user_id != '') AS has_userid,
  COUNTIF(log.user_id IS NULL OR log.user_id = '') AS no_userid,
  SAFE_DIVIDE(COUNTIF(log.user_id IS NOT NULL AND log.user_id != ''), COUNT(*)) AS userid_rate
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date BETWEEN '2026-04-01' AND '2026-04-07'
  AND log.app_id IN ('BEC3799A1CB6EE4D97BF451CB4EB8408', '4925CFAA3FA0144A611342E2076552BA')
GROUP BY 1