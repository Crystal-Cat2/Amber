  -- 查询目的：基于 Hudi 的 adslog_load_latency 事件，
  -- 统计 AdMob 中排除既有 6 个 placement_id 后的剩余 waterfall placement 状态分布。
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
      AND b.ad_format IN ('banner', 'interstitial', 'rewarded')
  ),

  waterfall_rows AS (
    SELECT
      product,
      target_version,
      ad_format,
      placement_id,
      COALESCE(fill_status_code, '') AS fill_status_code
    FROM admob_rows
    WHERE placement_id IS NOT NULL
      AND placement_id != ''
      AND placement_id NOT IN (
        'ca-app-pub-9205389740674078/1370451014',
        'ca-app-pub-9205389740674078/8776645237',
        'ca-app-pub-9205389740674078/6562140933',
        'ca-app-pub-9205389740674078/6135800336',
        'ca-app-pub-9205389740674078/3484912845',
        'ca-app-pub-9205389740674078/2985858001'
      )
  )

  SELECT
    product,
    target_version,
    ad_format,
    placement_id,
    COUNT(*) AS waterfall_latency_cnt,
    COUNTIF(fill_status_code = '-1') AS status_minus_1_cnt,
    COUNTIF(fill_status_code = '-2') AS status_minus_2_cnt,
    COUNTIF(fill_status_code = '-3') AS status_minus_3_cnt,
    COUNTIF(fill_status_code NOT IN ('-1', '-2', '-3')) AS other_status_cnt,
    SAFE_DIVIDE(COUNTIF(fill_status_code = '-1'), COUNT(*)) AS status_minus_1_ratio,
    SAFE_DIVIDE(COUNTIF(fill_status_code = '-2'), COUNT(*)) AS status_minus_2_ratio,
    SAFE_DIVIDE(COUNTIF(fill_status_code = '-3'), COUNT(*)) AS status_minus_3_ratio
  FROM waterfall_rows
  GROUP BY
    product,
    target_version,
    ad_format,
    placement_id
  ORDER BY
    product,
    ad_format,
    waterfall_latency_cnt DESC,
    placement_id;