-- 验证各 ID 在 Android (ball_sort) 和 iOS (ios_nuts_sort) 的填充率
-- 用于确认 business-rules.md 中 ID 使用规则的准确性
SELECT
  'ball_sort' AS product,
  COUNTIF(user_pseudo_id IS NOT NULL AND user_pseudo_id != '') AS has_upid,
  COUNTIF(user_id IS NOT NULL AND user_id != '') AS has_uid,
  COUNTIF(device.vendor_id IS NOT NULL AND device.vendor_id != '') AS has_vendor_id,
  COUNTIF(device.advertising_id IS NOT NULL AND device.advertising_id != '') AS has_adid,
  COUNT(*) AS total
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date = '2026-04-01' AND event_name = 'user_engagement'

UNION ALL

SELECT
  'ios_nuts_sort' AS product,
  COUNTIF(user_pseudo_id IS NOT NULL AND user_pseudo_id != '') AS has_upid,
  COUNTIF(user_id IS NOT NULL AND user_id != '') AS has_uid,
  COUNTIF(device.vendor_id IS NOT NULL AND device.vendor_id != '') AS has_vendor_id,
  COUNTIF(device.advertising_id IS NOT NULL AND device.advertising_id != '') AS has_adid,
  COUNT(*) AS total
FROM `transferred.hudi_ods.ios_nuts_sort`
WHERE event_date = '2026-04-01' AND event_name = 'user_engagement'
