-- Check: does tradplus_rt device_ifa match hudi user_id format?
SELECT
  t.device_ifa,
  h.user_id,
  h.advertising_id
FROM (
  SELECT DISTINCT LOWER(log.device_ifa) AS device_ifa
  FROM `transferred.dwd.dwd_tradplus_rt`
  WHERE date = DATE '2026-04-01'
    AND log.app_id = '4925CFAA3FA0144A611342E2076552BA'
    AND log.device_ifa IS NOT NULL AND log.device_ifa != ''
  LIMIT 5
) AS t
LEFT JOIN (
  SELECT DISTINCT
    user_id,
    LOWER(device.advertising_id) AS advertising_id
  FROM `transferred.hudi_ods.car_jam`
  WHERE event_date = '2026-04-01'
    AND event_name = 'lib_tpx_group'
    AND user_id IS NOT NULL
) AS h
ON t.device_ifa = h.advertising_id