-- 0401-0403 期间新增用户在第5-7关分关卡的胜利率
WITH new_users AS (
  SELECT DISTINCT user_id
  FROM `transferred.hudi_ods.car_jam`
  WHERE event_date BETWEEN '2026-04-01' AND '2026-04-03'
    AND event_name = 'first_open'
)
SELECT
  ep.value.string_value AS levelid,
  COUNT(DISTINCT IF(t.event_name = 'game_new_start', t.user_id, NULL)) AS start_uv,
  COUNT(DISTINCT IF(t.event_name = 'game_win', t.user_id, NULL)) AS win_uv,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(t.event_name = 'game_win', t.user_id, NULL)),
    COUNT(DISTINCT IF(t.event_name = 'game_new_start', t.user_id, NULL))
  ) AS win_rate
FROM `transferred.hudi_ods.car_jam` t,
  UNNEST(t.event_params) AS ep
INNER JOIN new_users nu ON t.user_id = nu.user_id
WHERE t.event_date BETWEEN '2026-04-01' AND '2026-04-03'
  AND t.event_name IN ('game_new_start', 'game_win')
  AND ep.key = 'levelid'
  AND ep.value.string_value IN ('5', '6', '7')
GROUP BY levelid
ORDER BY levelid
