SELECT
  device.advertising_id,
  user_id
FROM `transferred.hudi_ods.car_jam`
WHERE event_date = '2026-04-01'
  AND event_name = 'lib_tpx_group'
  AND user_id IS NOT NULL
  AND device.advertising_id IS NOT NULL
  AND device.advertising_id != ''
LIMIT 10