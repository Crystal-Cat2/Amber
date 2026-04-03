WITH base_rows AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    COALESCE(NULLIF(TRIM(country), ''), 'UNKNOWN') AS country,
    COALESCE(NULLIF(TRIM(max_unit_id), ''), 'UNKNOWN_UNIT') AS max_unit_id,
    user_pseudo_id,
    request_id,
    status,
    COALESCE(NULLIF(TRIM(network_type), ''), 'UNKNOWN') AS network_type,
    COALESCE(NULLIF(TRIM(network), ''), 'UNKNOWN') AS network,
    COALESCE(NULLIF(TRIM(placement_id), ''), 'UNKNOWN') AS placement_id,
    ecpm,
    event_timestamp
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') BETWEEN '2026-01-05' AND '2026-01-12'
    AND experiment_group IN ('no_is_adx', 'have_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND user_pseudo_id IS NOT NULL
    AND request_id IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
),
dedup_rows AS (
  SELECT * EXCEPT(status_row_num)
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id, status, network_type, network, placement_id, COALESCE(CAST(ecpm AS STRING), 'NULL')
        ORDER BY event_timestamp ASC
      ) AS status_row_num
    FROM base_rows
  )
  WHERE status_row_num = 1
),
all_status_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT CONCAT(network_type, '||', network)) AS network_cnt,
    COUNT(*) AS placement_cnt
  FROM dedup_rows
  WHERE status IN ('AD_LOADED', 'FAILED_TO_LOAD', 'AD_LOAD_NOT_ATTEMPTED')
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),
first_success_row AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    max_unit_id,
    user_pseudo_id,
    request_id,
    network_type AS success_network_type,
    network AS success_network,
    placement_id AS success_placement_id,
    ecpm,
    event_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id
      ORDER BY event_timestamp ASC
    ) AS success_row_num
  FROM dedup_rows
  WHERE status = 'AD_LOADED'
),
success_request_facts AS (
  SELECT
    success.experiment_group,
    success.product,
    success.ad_format,
    success.country,
    success.max_unit_id,
    success.user_pseudo_id,
    success.request_id,
    counts.network_cnt,
    counts.placement_cnt,
    success.success_network_type,
    success.success_network,
    success.success_placement_id,
    success.ecpm
  FROM first_success_row AS success
  JOIN all_status_counts AS counts
    ON success.experiment_group = counts.experiment_group
   AND success.product = counts.product
   AND success.ad_format = counts.ad_format
   AND success.user_pseudo_id = counts.user_pseudo_id
   AND success.request_id = counts.request_id
  WHERE success_row_num = 1
)
SELECT
  product,
  ad_format,
  experiment_group,
  country,
  max_unit_id,
  COUNT(*) AS success_request_cnt
FROM success_request_facts
GROUP BY product, ad_format, experiment_group, country, max_unit_id
ORDER BY product, ad_format, experiment_group, country, max_unit_id;
