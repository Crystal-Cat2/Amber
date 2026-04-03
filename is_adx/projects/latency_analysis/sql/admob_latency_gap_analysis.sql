-- section: version_coverage
-- 查询目的：按产品统计主版本覆盖，只展示每个产品 DAU 前 5 的版本。
WITH version_events AS (
  SELECT
    'screw_puzzle' AS product,
    COALESCE(app_info.version, 'unknown') AS app_version,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('user_engagement', 'adslog_load_latency')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    COALESCE(app_info.version, 'unknown') AS app_version,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('user_engagement', 'adslog_load_latency')
),
active_users AS (
  SELECT
    product,
    app_version,
    COUNT(DISTINCT user_pseudo_id) AS dau_user_cnt
  FROM version_events
  WHERE event_name = 'user_engagement'
  GROUP BY product, app_version
),
latency_users AS (
  SELECT
    product,
    app_version,
    COUNT(DISTINCT user_pseudo_id) AS latency_user_cnt
  FROM version_events
  WHERE event_name = 'adslog_load_latency'
  GROUP BY product, app_version
),
version_totals AS (
  SELECT
    product,
    SUM(dau_user_cnt) AS total_dau_user_cnt,
    SUM(latency_user_cnt) AS total_latency_user_cnt
  FROM (
    SELECT product, app_version, dau_user_cnt, 0 AS latency_user_cnt
    FROM active_users
    UNION ALL
    SELECT product, app_version, 0 AS dau_user_cnt, latency_user_cnt
    FROM latency_users
  )
  GROUP BY product
),
ranked_versions AS (
  SELECT
    a.product,
    a.app_version,
    a.dau_user_cnt,
    COALESCE(l.latency_user_cnt, 0) AS latency_user_cnt,
    SAFE_DIVIDE(COALESCE(l.latency_user_cnt, 0), a.dau_user_cnt) AS latency_user_coverage_ratio,
    SAFE_DIVIDE(a.dau_user_cnt, t.total_dau_user_cnt) AS dau_user_share,
    SAFE_DIVIDE(COALESCE(l.latency_user_cnt, 0), NULLIF(t.total_latency_user_cnt, 0)) AS latency_user_share,
    ROW_NUMBER() OVER (
      PARTITION BY a.product
      ORDER BY a.dau_user_cnt DESC, a.app_version
    ) AS version_rank
  FROM active_users a
  LEFT JOIN latency_users l
    ON a.product = l.product
   AND a.app_version = l.app_version
  LEFT JOIN version_totals t
    ON a.product = t.product
)
SELECT
  product,
  app_version,
  dau_user_cnt,
  latency_user_cnt,
  latency_user_coverage_ratio,
  dau_user_share,
  latency_user_share
FROM ranked_versions
WHERE version_rank <= 5
ORDER BY product, dau_user_cnt DESC, app_version;

-- section: request_latency_match
-- 查询目的：只看安卓 1.16.0 和 iOS 1.15.0，按 request_id 统计 adslog_request 与 adslog_load_latency 的匹配情况。
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
    ) AS request_id
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
    ) AS request_id
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name IN ('adslog_request', 'adslog_load_latency')
),
request_keys AS (
  SELECT
    product,
    target_version,
    ad_format,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_request'
    AND request_id IS NOT NULL
  GROUP BY product, target_version, ad_format, user_pseudo_id, request_id
),
latency_keys AS (
  SELECT
    product,
    target_version,
    ad_format,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_load_latency'
    AND request_id IS NOT NULL
  GROUP BY product, target_version, ad_format, user_pseudo_id, request_id
)
SELECT
  COALESCE(r.product, l.product) AS product,
  COALESCE(r.target_version, l.target_version) AS target_version,
  COALESCE(r.ad_format, l.ad_format) AS ad_format,
  COUNTIF(r.request_id IS NOT NULL) AS request_cnt,
  COUNTIF(l.request_id IS NOT NULL) AS latency_request_cnt,
  COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL) AS matched_request_cnt,
  COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NULL) AS request_without_latency_cnt,
  COUNTIF(r.request_id IS NULL AND l.request_id IS NOT NULL) AS latency_without_request_cnt,
  SAFE_DIVIDE(
    COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL),
    NULLIF(COUNTIF(r.request_id IS NOT NULL), 0)
  ) AS request_match_rate,
  SAFE_DIVIDE(
    COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL),
    NULLIF(COUNTIF(l.request_id IS NOT NULL), 0)
  ) AS latency_backfill_rate
FROM request_keys r
FULL OUTER JOIN latency_keys l
  ON r.product = l.product
 AND r.target_version = l.target_version
 AND r.ad_format = l.ad_format
 AND r.user_pseudo_id = l.user_pseudo_id
 AND r.request_id = l.request_id
GROUP BY
  COALESCE(r.product, l.product),
  COALESCE(r.target_version, l.target_version),
  COALESCE(r.ad_format, l.ad_format)
ORDER BY product, ad_format;

-- section: admob_backend_compare
-- 查询目的：只看安卓 1.16.0 和 iOS 1.15.0，统计指定 AdMob placement 的 started 口径，
-- 同时把 total_minus_not_started 改成“全部 latency 事件数减去 AdMob 未发起数”。
WITH all_latency_base AS (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.16.0'
    AND event_name = 'adslog_load_latency'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    '1.15.0' AS target_version,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name = 'adslog_load_latency'
),
all_latency_status_rows AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    CASE
      WHEN lib_net_status = 'network-null' THEN 'offline'
      WHEN lib_net_status = 'network-unknown' OR lib_net_status = '' THEN 'unknown'
      ELSE 'online'
    END AS network_status_group
  FROM all_latency_base
),
admob_rows AS (
  SELECT
    b.product,
    b.target_version,
    b.ad_format,
    b.lib_net_status,
    CASE
      WHEN b.lib_net_status = 'network-null' THEN 'offline'
      WHEN b.lib_net_status = 'network-unknown' OR b.lib_net_status = '' THEN 'unknown'
      ELSE 'online'
    END AS network_status_group,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(0)] AS placement_id,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(2)] AS fill_status_code
  FROM all_latency_base b,
  UNNEST(b.event_params) AS ep
  WHERE STARTS_WITH(ep.key, 'AdMob_')
),
admob_filtered_rows AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    network_status_group,
    fill_status_code
  FROM admob_rows
  WHERE placement_id IN (
    'ca-app-pub-9205389740674078/1370451014',
    'ca-app-pub-9205389740674078/8776645237',
    'ca-app-pub-9205389740674078/6562140933',
    'ca-app-pub-9205389740674078/6135800336',
    'ca-app-pub-9205389740674078/3484912845',
    'ca-app-pub-9205389740674078/2985858001'
  )
),
admob_not_started_counts AS (
  SELECT
    product,
    target_version,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS admob_not_started_cnt
  FROM admob_filtered_rows
  GROUP BY product, target_version, ad_format
),
admob_started_scope_all AS (
  SELECT
    product,
    target_version,
    ad_format,
    'all_network_status' AS scope_name,
    COUNT(*) AS admob_started_cnt
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
  GROUP BY product, target_version, ad_format
),
admob_started_scope_online AS (
  SELECT
    product,
    target_version,
    ad_format,
    'online_only' AS scope_name,
    COUNT(*) AS admob_started_cnt
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
    AND network_status_group = 'online'
  GROUP BY product, target_version, ad_format
),
all_latency_scope_all AS (
  SELECT
    product,
    target_version,
    ad_format,
    'all_network_status' AS scope_name,
    COUNT(*) AS all_latency_cnt
  FROM all_latency_status_rows
  GROUP BY product, target_version, ad_format
),
all_latency_scope_online AS (
  SELECT
    product,
    target_version,
    ad_format,
    'online_only' AS scope_name,
    COUNT(*) AS all_latency_cnt
  FROM all_latency_status_rows
  WHERE network_status_group = 'online'
  GROUP BY product, target_version, ad_format
),
comparison_base AS (
  SELECT
    a.product,
    a.target_version,
    a.ad_format,
    a.scope_name,
    a.admob_started_cnt,
    l.all_latency_cnt - COALESCE(n.admob_not_started_cnt, 0) AS admob_total_minus_not_started_cnt
  FROM (
    SELECT * FROM admob_started_scope_all
    UNION ALL
    SELECT * FROM admob_started_scope_online
  ) a
  JOIN (
    SELECT * FROM all_latency_scope_all
    UNION ALL
    SELECT * FROM all_latency_scope_online
  ) l
    ON a.product = l.product
   AND a.target_version = l.target_version
   AND a.ad_format = l.ad_format
   AND a.scope_name = l.scope_name
  LEFT JOIN admob_not_started_counts n
    ON a.product = n.product
   AND a.target_version = n.target_version
   AND a.ad_format = n.ad_format
),
started_network_breakdown AS (
  SELECT
    product,
    target_version,
    ad_format,
    network_status_group,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
  GROUP BY product, target_version, ad_format, network_status_group
),
total_minus_not_started_network_breakdown AS (
  SELECT
    product,
    target_version,
    ad_format,
    network_status_group,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM all_latency_status_rows
  GROUP BY product, target_version, ad_format, network_status_group
),
started_online_status_detail AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
    AND network_status_group = 'online'
  GROUP BY product, target_version, ad_format, lib_net_status
),
total_minus_not_started_online_status_detail AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM all_latency_status_rows
  WHERE network_status_group = 'online'
  GROUP BY product, target_version, ad_format, lib_net_status
)
SELECT
  'comparison_base' AS report_section,
  product,
  target_version,
  ad_format,
  scope_name,
  NULL AS basis_name,
  NULL AS status_name,
  NULL AS pv_count,
  NULL AS pv_ratio,
  admob_started_cnt,
  admob_total_minus_not_started_cnt
FROM comparison_base

UNION ALL

SELECT
  'network_breakdown' AS report_section,
  product,
  target_version,
  ad_format,
  'all_network_status' AS scope_name,
  'admob_started_cnt' AS basis_name,
  network_status_group AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM started_network_breakdown

UNION ALL

SELECT
  'network_breakdown' AS report_section,
  product,
  target_version,
  ad_format,
  'all_network_status' AS scope_name,
  'admob_total_minus_not_started_cnt' AS basis_name,
  network_status_group AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM total_minus_not_started_network_breakdown

UNION ALL

SELECT
  'online_status_detail' AS report_section,
  product,
  target_version,
  ad_format,
  'online_only' AS scope_name,
  'admob_started_cnt' AS basis_name,
  lib_net_status AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM started_online_status_detail

UNION ALL

SELECT
  'online_status_detail' AS report_section,
  product,
  target_version,
  ad_format,
  'online_only' AS scope_name,
  'admob_total_minus_not_started_cnt' AS basis_name,
  lib_net_status AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM total_minus_not_started_online_status_detail

ORDER BY report_section, product, ad_format, scope_name, basis_name, status_name;
