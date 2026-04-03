SELECT log.device_ifa
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date = DATE '2026-04-01'
  AND log.app_id = '4925CFAA3FA0144A611342E2076552BA'
  AND log.device_ifa IS NOT NULL
  AND log.device_ifa != ''
LIMIT 5