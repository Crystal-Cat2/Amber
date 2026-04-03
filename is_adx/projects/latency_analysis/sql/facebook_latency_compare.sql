-- section: facebook_backend_compare
-- 查询目的：沿用 admob_latency 第三部分的数据源与状态码口径，只把渠道从 AdMob 改成 Facebook，
-- 且不再保留网络状态拆分，只输出 Facebook 渠道的两套核心对比指标。
WITH
-- 全量 latency 基表：继续使用原第三部分的时间窗和 adslog_load_latency 事件，
-- 但不再限制 app 版本，也不输出版本维度。
all_latency_base AS (
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

-- 全量 latency 总数：这里的“总数”是所有渠道的总数，不限制 Facebook。
all_latency_counts AS (
  SELECT
    product,
    ad_format,
    COUNT(*) AS latency_total_cnt
  FROM all_latency_base
  WHERE ad_format IN ('banner', 'interstitial', 'rewarded')
  GROUP BY
    product,
    ad_format
),

-- Facebook 渠道明细：继续沿用原第三部分的 event_params 展开方式，只把渠道前缀改成 Facebook。
facebook_rows AS (
  SELECT
    b.product,
    b.ad_format,
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
  WHERE STARTS_WITH(ep.key, 'Facebook_')
    AND b.ad_format IN ('banner', 'interstitial', 'rewarded')
),

-- Facebook 的 started 口径：只统计 status = -2 / -3。
facebook_started_counts AS (
  SELECT
    product,
    ad_format,
    COUNTIF(fill_status_code IN ('-2', '-3')) AS facebook_started_cnt
  FROM facebook_rows
  GROUP BY
    product,
    ad_format
),

-- Facebook 的 not_started 口径：只统计 status = -1，后续用“全部总数 - not_started”。
facebook_not_started_counts AS (
  SELECT
    product,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS facebook_not_started_cnt
  FROM facebook_rows
  GROUP BY
    product,
    ad_format
),

-- 最终对比基表：started 仍是 Facebook 渠道内的 -2 / -3，
-- total_minus_not_started 则是“全部 latency 总数 - Facebook 的 -1 数量”。
facebook_compare_base AS (
  SELECT
    t.product,
    t.ad_format,
    COALESCE(s.facebook_started_cnt, 0) AS facebook_started_cnt,
    t.latency_total_cnt - COALESCE(n.facebook_not_started_cnt, 0) AS facebook_total_minus_not_started_cnt
  FROM all_latency_counts t
  LEFT JOIN facebook_started_counts s
    ON t.product = s.product
   AND t.ad_format = s.ad_format
  LEFT JOIN facebook_not_started_counts n
    ON t.product = n.product
   AND t.ad_format = n.ad_format
)

SELECT
  product,
  ad_format,
  facebook_started_cnt,
  facebook_total_minus_not_started_cnt
FROM facebook_compare_base
ORDER BY
  product,
  ad_format;
