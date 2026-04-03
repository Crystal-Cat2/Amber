-- 查询目的：
-- 统计 2026-01-05 到 2026-01-12 期间，IsAdxCustomAdapter 在 request 粒度的 latency 分布，
-- 输出到 product + ad_format + max_unit_id + request_status + experiment_group + 0.01 秒原始桶。
--
-- 输出字段：
-- product, ad_format, max_unit_id, request_status, latency_bucket_raw,
-- experiment_group, request_pv, denominator_request_pv, share
--
-- 关键口径：
-- 1. 只使用 `commercial-adx.lmh.isadx_adslog_latency_detail`，且仅统计 network = 'IsAdxCustomAdapter'。
-- 2. request 粒度定义为 product + ad_format + experiment_group + user_pseudo_id + request_id + max_unit_id。
-- 3. request_latency_sec = 同一 request 内所有 isadx 行的 latency 加总，latency 单位已是秒。
-- 4. request_status：
--    - success：当前 request 内存在任意 AD_LOADED
--    - fail：当前 request 内不存在 AD_LOADED，但存在 FAILED_TO_LOAD
--    - 其他情况不纳入结果
-- 5. 底层 SQL 保留原始 0.01 秒桶，不直接固化展示分桶。

WITH base_isadx AS (
  SELECT
    product,
    ad_format,
    experiment_group,
    user_pseudo_id,
    request_id,
    max_unit_id,
    latency,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE network = 'IsAdxCustomAdapter'
    AND event_time_utc >= TIMESTAMP('2026-01-05 00:00:00+00')
    AND event_time_utc < TIMESTAMP('2026-01-13 00:00:00+00')
    AND experiment_group IN ('no_is_adx', 'have_is_adx')
    AND request_id IS NOT NULL
    AND request_id != ''
    AND max_unit_id IS NOT NULL
    AND max_unit_id != ''
    AND user_pseudo_id IS NOT NULL
    AND ad_format IN ('interstitial', 'rewarded')
),

request_level AS (
  SELECT
    product,
    ad_format,
    experiment_group,
    user_pseudo_id,
    request_id,
    max_unit_id,
    SUM(latency) AS request_latency_sec,
    MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_loaded,
    MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) AS has_failed
  FROM base_isadx
  GROUP BY product, ad_format, experiment_group, user_pseudo_id, request_id, max_unit_id
),

request_statused AS (
  SELECT
    product,
    ad_format,
    experiment_group,
    max_unit_id,
    ROUND(request_latency_sec, 2) AS rounded_latency_sec,
    CASE
      WHEN has_loaded = 1 THEN 'success'
      WHEN has_loaded = 0 AND has_failed = 1 THEN 'fail'
    END AS request_status
  FROM request_level
),

filtered_requests AS (
  SELECT
    product,
    ad_format,
    experiment_group,
    max_unit_id,
    request_status,
    FORMAT('%.2f', rounded_latency_sec) AS latency_bucket_raw
  FROM request_statused
  WHERE request_status IS NOT NULL
)

SELECT
  product,
  ad_format,
  max_unit_id,
  request_status,
  latency_bucket_raw,
  experiment_group,
  COUNT(*) AS request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY product, ad_format, max_unit_id, request_status, experiment_group
  ) AS denominator_request_pv,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (
      PARTITION BY product, ad_format, max_unit_id, request_status, experiment_group
    )
  ) AS share
FROM filtered_requests
GROUP BY product, ad_format, max_unit_id, request_status, latency_bucket_raw, experiment_group
ORDER BY
  product,
  ad_format,
  max_unit_id,
  request_status,
  experiment_group,
  SAFE_CAST(latency_bucket_raw AS FLOAT64);
