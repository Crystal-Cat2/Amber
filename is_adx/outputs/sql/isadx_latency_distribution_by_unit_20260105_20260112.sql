-- 查询目的：
-- 统计 2026-01-05 到 2026-01-12 期间，IsAdxCustomAdapter 在 request 粒度的 latency 分布。
--
-- 输出字段：
-- product, ad_format, max_unit_id, request_status, latency_bucket, request_pv
--
-- 关键口径：
-- 1. 只使用 `commercial-adx.lmh.isadx_adslog_latency_detail`，且仅统计 network = 'IsAdxCustomAdapter'。
-- 2. request 粒度定义为 product + ad_format + user_pseudo_id + request_id + max_unit_id。
-- 3. request_latency_sec = 同一 request 内所有 isadx 行的 latency 加总，latency 单位已是秒。
-- 4. request_status:
--    - success：当前 request 内存在任意 AD_LOADED
--    - fail：当前 request 内不存在 AD_LOADED，但存在 FAILED_TO_LOAD
--    - 其他情况不纳入结果
-- 5. 分布桶：
--    - ROUND(request_latency_sec, 2) 后，0.00 到 30.00 保留两位小数字符串
--    - 大于 30.00 秒统一归为 30+

WITH
-- 1. 先筛出时间窗内的 isadx 明细，只保留 request / unit 可识别的记录
base_isadx AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    max_unit_id,
    latency,
    status
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE network = 'IsAdxCustomAdapter'
    AND event_time_utc >= TIMESTAMP('2026-01-05 00:00:00+00')
    AND event_time_utc < TIMESTAMP('2026-01-13 00:00:00+00')
    AND request_id IS NOT NULL
    AND request_id != ''
    AND max_unit_id IS NOT NULL
    AND max_unit_id != ''
    AND user_pseudo_id IS NOT NULL
    AND ad_format IS NOT NULL
),

-- 2. 聚到 request 粒度，累计 isadx latency，并标记是否命中过成功 / 失败
request_level AS (
  SELECT
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    max_unit_id,
    SUM(latency) AS request_latency_sec,
    MAX(CASE WHEN status = 'AD_LOADED' THEN 1 ELSE 0 END) AS has_loaded,
    MAX(CASE WHEN status = 'FAILED_TO_LOAD' THEN 1 ELSE 0 END) AS has_failed
  FROM base_isadx
  GROUP BY product, ad_format, user_pseudo_id, request_id, max_unit_id
),

-- 3. 生成 request 级 success / fail 状态，其余 request 不进入结果
request_statused AS (
  SELECT
    product,
    ad_format,
    max_unit_id,
    request_id,
    user_pseudo_id,
    request_latency_sec,
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
    max_unit_id,
    request_id,
    user_pseudo_id,
    request_latency_sec,
    request_status
  FROM request_statused
  WHERE request_status IS NOT NULL
),

-- 4. 先把 request 级总时长四舍五入到两位小数
rounded_requests AS (
  SELECT
    product,
    ad_format,
    max_unit_id,
    request_status,
    ROUND(request_latency_sec, 2) AS rounded_latency_sec
  FROM filtered_requests
),

-- 5. 再按 30 秒上限折叠分桶
bucketed_requests AS (
  SELECT
    product,
    ad_format,
    max_unit_id,
    request_status,
    rounded_latency_sec,
    CASE
      WHEN rounded_latency_sec > 30.00 THEN '30+'
      ELSE FORMAT('%.2f', rounded_latency_sec)
    END AS latency_bucket
  FROM rounded_requests
)

SELECT
  product,
  ad_format,
  max_unit_id,
  request_status,
  latency_bucket,
  COUNT(*) AS request_pv
FROM bucketed_requests
GROUP BY product, ad_format, max_unit_id, request_status, latency_bucket
ORDER BY
  product,
  ad_format,
  max_unit_id,
  request_status,
  CASE
    WHEN latency_bucket = '30+' THEN 999999
    ELSE SAFE_CAST(latency_bucket AS FLOAT64)
  END;
