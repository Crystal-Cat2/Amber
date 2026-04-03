-- 查询目的：统计 loaded_placement_cnt = 0 的 latency 请求中，有对应 Hudi adslog_filled 事件的请求数汇总。
-- 输出字段：experiment_group, product, ad_format, zero_loaded_request_cnt, matched_adslog_filled_request_cnt, unmatched_request_cnt, matched_ratio
-- 关键口径：
-- 1. latency 成功定义为 status = 'AD_LOADED'。
-- 2. 在单次 request_id 内，对成功的 placement_id 做去重计数；loaded_placement_cnt = 0 表示该请求没有任何成功 placement。
-- 3. Hudi 侧事件使用正式事件名 adslog_filled，并按同一 product + user_pseudo_id + request_id 关联。
-- 4. 汇总维度保持 experiment_group + product + ad_format。

-- 步骤 1：先按 request 粒度统计 latency 中成功 placement 的去重个数
WITH per_request_loaded_placements AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT IF(status = 'AD_LOADED', placement_id, NULL)) AS loaded_placement_cnt
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
    AND DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
  GROUP BY
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id
),

-- 步骤 2：筛出 loaded_placement_cnt = 0 的 latency 请求
zero_loaded_requests AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id
  FROM per_request_loaded_placements
  WHERE loaded_placement_cnt = 0
),

-- 步骤 3：提取 Hudi 中的 adslog_filled 请求，统一成与 latency 一致的 product 命名
hudi_adslog_filled_requests AS (
  SELECT
    'com.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    user_pseudo_id,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      WHEN event_name LIKE 'inter%' THEN 'interstitial'
      WHEN event_name LIKE 'reward%' THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_filled'
),

-- 步骤 4：对 Hudi filled 请求按 request 粒度去重
hudi_adslog_filled_dedup AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id
  FROM hudi_adslog_filled_requests
  WHERE ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
  GROUP BY
    product,
    ad_format,
    user_pseudo_id,
    request_id
)

-- 步骤 5：汇总 zero_loaded 请求中，能匹配到 adslog_filled 的数量与占比
SELECT
  z.experiment_group,
  z.product,
  z.ad_format,
  COUNT(*) AS zero_loaded_request_cnt,
  COUNTIF(h.request_id IS NOT NULL) AS matched_adslog_filled_request_cnt,
  COUNTIF(h.request_id IS NULL) AS unmatched_request_cnt,
  SAFE_DIVIDE(
    COUNTIF(h.request_id IS NOT NULL),
    COUNT(*)
  ) AS matched_ratio
FROM zero_loaded_requests z
LEFT JOIN hudi_adslog_filled_dedup h
  ON z.product = h.product
  AND z.ad_format = h.ad_format
  AND z.user_pseudo_id = h.user_pseudo_id
  AND z.request_id = h.request_id
GROUP BY
  z.experiment_group,
  z.product,
  z.ad_format
ORDER BY
  z.product,
  z.ad_format,
  z.experiment_group;
