SELECT
  log.app_id,
  slim_name,
  COUNT(*) AS total_rows,
  COUNTIF(log.user_id IS NOT NULL AND log.user_id != '') AS has_userid,
  COUNTIF(log.device_ifa IS NOT NULL AND log.device_ifa != '') AS has_ifa,
  COUNTIF(log.device_idfv IS NOT NULL AND log.device_idfv != '') AS has_idfv
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date BETWEEN '2026-04-01' AND '2026-04-07'
  AND log.app_id IN ('BEC3799A1CB6EE4D97BF451CB4EB8408', '4925CFAA3FA0144A611342E2076552BA')
GROUP BY 1, 2