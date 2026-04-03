-- 查询目的：统计拧螺丝双端 adslog_request 的 rewarded 请求，在同一用户同一广告格式下相邻请求的时间差分布，
-- 并输出各时间差 PV 占有效总 PV 的比例。
-- 输出字段：
-- 1. product：产品 / 端
-- 2. ad_format：广告格式，固定为 rewarded
-- 3. interval_bucket：<1000ms 时保留 3 位小数毫秒原值，>=1000ms 统一归到 1000ms+
-- 4. bucket_pv：当前时间差的 PV
-- 5. total_valid_pv：当前 product + ad_format 下有效时间差总 PV
-- 6. pv_ratio：bucket_pv / total_valid_pv
-- 关键口径：
-- 1. 时间范围固定为 2026-01-05 到 2026-01-12。
-- 2. 仅使用 Hudi adslog_request 事件，且必须同时带 event_date 与 event_name 分区过滤。
-- 3. “同个用户”按数仓规范构造 user_key，不直接使用 user_pseudo_id。
-- 4. 相邻请求排序仅使用 event_timestamp。
-- 5. 时间差基于 event_timestamp 原始微秒值计算；<1000ms 保留 3 位小数原值，>=1000ms 合并为 1000ms+。
-- 6. 仅统计能形成相邻请求时间差的有效请求对；user_key 缺失的记录不参与统计。

SELECT
  product,
  ad_format,
  interval_bucket,
  bucket_pv,
  SUM(bucket_pv) OVER (PARTITION BY product, ad_format) AS total_valid_pv,
  SAFE_DIVIDE(bucket_pv, SUM(bucket_pv) OVER (PARTITION BY product, ad_format)) AS pv_ratio
FROM (
  SELECT
    product,
    ad_format,
    interval_bucket,
    interval_bucket_sort_value,
    COUNT(*) AS bucket_pv
  FROM (
    SELECT
      product,
      ad_format,
      CASE
        WHEN diff_ms >= 1000 THEN '1000ms+'
        ELSE FORMAT('%.3fms', diff_ms)
      END AS interval_bucket,
      CASE
        WHEN diff_ms >= 1000 THEN NULL
        ELSE diff_ms
      END AS interval_bucket_sort_value
    FROM (
      SELECT
        'screw_puzzle' AS product,
        'rewarded' AS ad_format,
        ROUND((next_event_timestamp - event_timestamp) / 1000.0, 3) AS diff_ms
      FROM (
        SELECT
          user_key,
          event_timestamp,
          LEAD(event_timestamp) OVER (
            PARTITION BY user_key
            ORDER BY event_timestamp
          ) AS next_event_timestamp
        FROM (
          SELECT
            CONCAT(
              NULLIF(device.advertising_id, ''),
              '-',
              CAST(user_first_touch_timestamp AS STRING)
            ) AS user_key,
            event_timestamp
          FROM (
            SELECT
              device,
              user_first_touch_timestamp,
              event_timestamp,
              MAX(IF(param.key = 'ad_format', param.value.int_value, NULL)) AS ad_format_code
            FROM `transferred.hudi_ods.screw_puzzle`
            LEFT JOIN UNNEST(event_params.array) AS param
            WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
              AND event_name = 'adslog_request'
            GROUP BY
              device,
              user_first_touch_timestamp,
              event_timestamp
          ) android_reward_base
          WHERE ad_format_code = 2
            AND CONCAT(
              NULLIF(device.advertising_id, ''),
              '-',
              CAST(user_first_touch_timestamp AS STRING)
            ) IS NOT NULL
            AND event_timestamp IS NOT NULL
        ) android_reward_requests
      ) android_reward_pairs
      WHERE next_event_timestamp IS NOT NULL
        AND next_event_timestamp >= event_timestamp

      UNION ALL

      SELECT
        'ios_screw_puzzle' AS product,
        'rewarded' AS ad_format,
        ROUND((next_event_timestamp - event_timestamp) / 1000.0, 3) AS diff_ms
      FROM (
        SELECT
          user_key,
          event_timestamp,
          LEAD(event_timestamp) OVER (
            PARTITION BY user_key
            ORDER BY event_timestamp
          ) AS next_event_timestamp
        FROM (
          SELECT
            CONCAT(
              NULLIF(COALESCE(user_id, device.vendor_id), ''),
              '_',
              CAST(user_first_touch_timestamp AS STRING)
            ) AS user_key,
            event_timestamp
          FROM (
            SELECT
              user_id,
              device,
              user_first_touch_timestamp,
              event_timestamp,
              MAX(IF(param.key = 'ad_format', param.value.int_value, NULL)) AS ad_format_code
            FROM `transferred.hudi_ods.ios_screw_puzzle`
            LEFT JOIN UNNEST(event_params.array) AS param
            WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
              AND event_name = 'adslog_request'
            GROUP BY
              user_id,
              device,
              user_first_touch_timestamp,
              event_timestamp
          ) ios_reward_base
          WHERE ad_format_code = 2
            AND CONCAT(
              NULLIF(COALESCE(user_id, device.vendor_id), ''),
              '_',
              CAST(user_first_touch_timestamp AS STRING)
            ) IS NOT NULL
            AND event_timestamp IS NOT NULL
        ) ios_reward_requests
      ) ios_reward_pairs
      WHERE next_event_timestamp IS NOT NULL
        AND next_event_timestamp >= event_timestamp
    ) interval_base
  ) bucketed
  GROUP BY
    product,
    ad_format,
    interval_bucket,
    interval_bucket_sort_value
) bucket_counts
ORDER BY
  product,
  ad_format,
  CASE WHEN interval_bucket = '1000ms+' THEN 1 ELSE 0 END,
  interval_bucket_sort_value;
