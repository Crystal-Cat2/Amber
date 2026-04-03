-- 查询目的：统计固定版本下 adslog_request 与 adslog_load_latency 的请求级网络状态分布。
-- 口径说明：按 product + ad_format + user_pseudo_id + request_id 去重，且排除空 request_id。
WITH all_events AS (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    user_pseudo_id,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.16.0'
    AND event_name IN ('adslog_request', 'adslog_load_latency')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    '1.15.0' AS target_version,
    user_pseudo_id,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name IN ('adslog_request', 'adslog_load_latency')
),
normalized_events AS (
  SELECT
    product,
    target_version,
    CASE
      WHEN event_name = 'adslog_request' THEN 'request'
      ELSE 'latency'
    END AS event_type,
    ad_format,
    user_pseudo_id,
    request_id,
    CASE
      WHEN lib_net_status = 'network-null' THEN '2-offline'
      WHEN lib_net_status = 'network-unknown' OR lib_net_status = '' THEN '1-unknown'
      ELSE '3-online'
    END AS network_status_token
  FROM all_events
),
deduplicated_requests AS (
  SELECT
    product,
    target_version,
    event_type,
    ad_format,
    user_pseudo_id,
    request_id,
    MAX(network_status_token) AS network_status_token
  FROM normalized_events
  WHERE request_id IS NOT NULL
  GROUP BY product, target_version, event_type, ad_format, user_pseudo_id, request_id
),
request_status_counts AS (
  SELECT
    product,
    target_version,
    event_type,
    ad_format,
    network_status_token,
    COUNT(*) AS request_cnt
  FROM deduplicated_requests
  GROUP BY product, target_version, event_type, ad_format, network_status_token
)
SELECT
  product,
  target_version,
  event_type,
  ad_format,
  SPLIT(network_status_token, '-')[SAFE_OFFSET(1)] AS network_status_group,
  request_cnt,
  SAFE_DIVIDE(request_cnt, NULLIF(SUM(request_cnt) OVER (PARTITION BY product, target_version, event_type, ad_format), 0)) AS request_ratio
FROM request_status_counts
ORDER BY
  product,
  event_type,
  ad_format,
  network_status_token DESC;
