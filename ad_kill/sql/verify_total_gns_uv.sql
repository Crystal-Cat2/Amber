-- 直接算有 AB 分组的第5关 game_new_start UV 总数
SELECT
  COUNT(DISTINCT user_pseudo_id) AS total_gns_users
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
  AND event_name = 'game_new_start'
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
  AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
  AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  AND REGEXP_CONTAINS(
    LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
    r'(^|[^a-z0-9])(12a|12b)([^a-z0-9]|$)'
  )