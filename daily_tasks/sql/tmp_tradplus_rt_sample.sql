SELECT
  log.date AS log_date,
  log.user_id,
  log.app_id,
  log.unit_id,
  log.ecpm,
  log.ecpm_cny,
  log.network_id,
  log.adsource_id,
  log.scene_id,
  slim_name,
  date AS partition_date
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date BETWEEN '2026-04-01' AND '2026-04-01'
  AND log.app_id IN ('BEC3799A1CB6EE4D97BF451CB4EB8408', '4925CFAA3FA0144A611342E2076552BA')
LIMIT 20