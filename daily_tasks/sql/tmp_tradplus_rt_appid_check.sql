SELECT
  log.app_id,
  slim_name,
  COUNT(*) AS total_rows
FROM `transferred.dwd.dwd_tradplus_rt`
WHERE date BETWEEN '2026-04-01' AND '2026-04-07'
  AND (
    log.app_id IN ('BEC3799A1CB6EE4D97BF451CB4EB8408', '4925CFAA3FA0144A611342E2076552BA')
    OR LOWER(slim_name) LIKE '%car_jam%'
    OR LOWER(slim_name) LIKE '%car jam%'
  )
GROUP BY 1, 2
ORDER BY total_rows DESC