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
FROM (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    CASE
      WHEN af.value.int_value = 0 THEN 'banner'
      WHEN af.value.int_value = 1 THEN 'interstitial'
      WHEN af.value.int_value = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(0)] AS placement_id,
    COALESCE(
      SPLIT(
        COALESCE(
          ep.value.string_value,
          CAST(ep.value.int_value AS STRING),
          CAST(ep.value.double_value AS STRING)
        ),
        '|'
      )[SAFE_OFFSET(2)],
      ''
    ) AS fill_status_code
  FROM `transferred.hudi_ods.screw_puzzle` t
  LEFT JOIN UNNEST(t.event_params.array) AS af
    ON af.key = 'ad_format'
  CROSS JOIN UNNEST(t.event_params.array) AS ep
  WHERE t.event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND t.app_info.version = '1.16.0'
    AND t.event_name = 'adslog_load_latency'
    AND STARTS_WITH(ep.key, 'AdMob_')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    '1.15.0' AS target_version,
    CASE
      WHEN af.value.int_value = 0 THEN 'banner'
      WHEN af.value.int_value = 1 THEN 'interstitial'
      WHEN af.value.int_value = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(0)] AS placement_id,
    COALESCE(
      SPLIT(
        COALESCE(
          ep.value.string_value,
          CAST(ep.value.int_value AS STRING),
          CAST(ep.value.double_value AS STRING)
        ),
        '|'
      )[SAFE_OFFSET(2)],
      ''
    ) AS fill_status_code
  FROM `transferred.hudi_ods.ios_screw_puzzle` t
  LEFT JOIN UNNEST(t.event_params.array) AS af
    ON af.key = 'ad_format'
  CROSS JOIN UNNEST(t.event_params.array) AS ep
  WHERE t.event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND t.app_info.version = '1.15.0'
    AND t.event_name = 'adslog_load_latency'
    AND STARTS_WITH(ep.key, 'AdMob_')
) admob_rows
WHERE ad_format IN ('banner', 'interstitial', 'rewarded')
  AND placement_id IS NOT NULL
  AND placement_id != ''
  AND placement_id NOT IN (
    'ca-app-pub-9205389740674078/1370451014',
    'ca-app-pub-9205389740674078/8776645237',
    'ca-app-pub-9205389740674078/6562140933',
    'ca-app-pub-9205389740674078/6135800336',
    'ca-app-pub-9205389740674078/3484912845',
    'ca-app-pub-9205389740674078/2985858001'
  )
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
