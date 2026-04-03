-- 查询目的：新增一份独立 SQL，统一输出 AdMob / Facebook 的 -1、-2、-3 状态码数量，
-- 以及不限制 network 的全部 latency 总数，供后续 Python 直接消费。
WITH
-- AdMob 全量 latency 基表：沿用原 SQL 的时间窗、目标版本和 adslog_load_latency 事件。
admob_latency_base AS (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
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
    event_params.array AS event_params
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name = 'adslog_load_latency'
),

-- AdMob 渠道明细：沿用原 SQL 的 6 个 placement 过滤，只保留目标广告类型。
admob_rows AS (
  SELECT
    b.product,
    b.target_version,
    b.ad_format,
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
  FROM admob_latency_base b,
  UNNEST(b.event_params) AS ep
  WHERE STARTS_WITH(ep.key, 'AdMob_')
    AND b.ad_format IN ('banner', 'interstitial', 'rewarded')
),

admob_filtered_rows AS (
  SELECT
    product,
    target_version,
    ad_format,
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

-- AdMob 基础计数：状态码只在渠道行内统计；总数来自全部 latency，不限制 network。
admob_status_counts AS (
  SELECT
    product,
    target_version,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS status_minus_1_cnt,
    COUNTIF(fill_status_code = '-2') AS status_minus_2_cnt,
    COUNTIF(fill_status_code = '-3') AS status_minus_3_cnt
  FROM admob_filtered_rows
  GROUP BY product, target_version, ad_format
),

admob_all_latency_counts AS (
  SELECT
    product,
    target_version,
    ad_format,
    COUNT(*) AS all_latency_total_cnt
  FROM admob_latency_base
  WHERE ad_format IN ('banner', 'interstitial', 'rewarded')
  GROUP BY product, target_version, ad_format
),

admob_summary AS (
  SELECT
    'admob' AS channel,
    t.product,
    t.target_version,
    t.ad_format,
    COALESCE(s.status_minus_1_cnt, 0) AS status_minus_1_cnt,
    COALESCE(s.status_minus_2_cnt, 0) AS status_minus_2_cnt,
    COALESCE(s.status_minus_3_cnt, 0) AS status_minus_3_cnt,
    t.all_latency_total_cnt
  FROM admob_all_latency_counts t
  LEFT JOIN admob_status_counts s
    ON t.product = s.product
   AND t.target_version = s.target_version
   AND t.ad_format = s.ad_format
),

-- Facebook 全量 latency 基表：沿用原 SQL 的时间窗与全版本口径，不输出具体版本。
facebook_latency_base AS (
  SELECT
    'screw_puzzle' AS product,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_load_latency'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_load_latency'
),

-- Facebook 渠道明细：只展开 Facebook_ 前缀参数，版本字段统一回写为 all_versions。
facebook_rows AS (
  SELECT
    b.product,
    'all_versions' AS target_version,
    b.ad_format,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(2)] AS fill_status_code
  FROM facebook_latency_base b,
  UNNEST(b.event_params) AS ep
  WHERE STARTS_WITH(ep.key, 'Facebook_')
    AND b.ad_format IN ('banner', 'interstitial', 'rewarded')
),

facebook_status_counts AS (
  SELECT
    product,
    target_version,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS status_minus_1_cnt,
    COUNTIF(fill_status_code = '-2') AS status_minus_2_cnt,
    COUNTIF(fill_status_code = '-3') AS status_minus_3_cnt
  FROM facebook_rows
  GROUP BY product, target_version, ad_format
),

facebook_all_latency_counts AS (
  SELECT
    product,
    'all_versions' AS target_version,
    ad_format,
    COUNT(*) AS all_latency_total_cnt
  FROM facebook_latency_base
  WHERE ad_format IN ('banner', 'interstitial', 'rewarded')
  GROUP BY product, ad_format
),

facebook_summary AS (
  SELECT
    'facebook' AS channel,
    t.product,
    t.target_version,
    t.ad_format,
    COALESCE(s.status_minus_1_cnt, 0) AS status_minus_1_cnt,
    COALESCE(s.status_minus_2_cnt, 0) AS status_minus_2_cnt,
    COALESCE(s.status_minus_3_cnt, 0) AS status_minus_3_cnt,
    t.all_latency_total_cnt
  FROM facebook_all_latency_counts t
  LEFT JOIN facebook_status_counts s
    ON t.product = s.product
   AND t.target_version = s.target_version
   AND t.ad_format = s.ad_format
),

combined_summary AS (
  SELECT
    channel,
    product,
    target_version,
    ad_format,
    status_minus_1_cnt,
    status_minus_2_cnt,
    status_minus_3_cnt,
    all_latency_total_cnt
  FROM admob_summary

  UNION ALL

  SELECT
    channel,
    product,
    target_version,
    ad_format,
    status_minus_1_cnt,
    status_minus_2_cnt,
    status_minus_3_cnt,
    all_latency_total_cnt
  FROM facebook_summary
)

SELECT
  channel,
  product,
  target_version,
  ad_format,
  status_minus_1_cnt,
  status_minus_2_cnt,
  status_minus_3_cnt,
  all_latency_total_cnt
FROM combined_summary
ORDER BY channel, product, target_version, ad_format;
