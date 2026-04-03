SELECT
  'hudi' AS source,
  MIN(event_timestamp) AS min_ts,
  MAX(event_timestamp) AS max_ts
FROM `transferred.hudi_ods.car_jam`
WHERE event_date = '2026-04-01'
  AND event_name = 'lib_tpx_group'
  AND user_id IS NOT NULL

UNION ALL

SELECT
  'tradplus_rt' AS source,
  MIN(log.ts) AS min_ts,
  MAX(log.ts) AS max_ts
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date = DATE '2026-04-01'
  AND log.app_id = '4925CFAA3FA0144A611342E2076552BA'
  AND log.device_ifa IS NOT NULL AND log.device_ifa != ''