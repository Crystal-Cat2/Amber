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
        PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id, status, network_type, network, placement_id
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

fail_counts AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(*) AS fail_cnt
  FROM dedup_rows
  WHERE status = 'FAILED_TO_LOAD'
  GROUP BY experiment_group, product, ad_format, user_pseudo_id, request_id
),

success_rank_rows AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    country,
    max_unit_id,
    user_pseudo_id,
    request_id,
    CASE
      WHEN LOWER(network_type) = 'bidding' THEN 'bidding'
      ELSE 'waterfall'
    END AS success_network_type,
    1 AS attempt_rank
  FROM dedup_rows
  WHERE status = 'AD_LOADED'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY experiment_group, product, ad_format, user_pseudo_id, request_id
    ORDER BY event_timestamp ASC
  ) = 1
),

success_request_facts AS (
  SELECT
    s.experiment_group,
    s.product,
    s.ad_format,
    s.country,
    s.max_unit_id,
    c.network_cnt,
    c.placement_cnt,
    s.success_network_type,
    COALESCE(f.fail_cnt, 0) + s.attempt_rank AS success_rank
  FROM success_rank_rows s
  JOIN all_status_counts c
    ON s.experiment_group = c.experiment_group
   AND s.product = c.product
   AND s.ad_format = c.ad_format
   AND s.user_pseudo_id = c.user_pseudo_id
   AND s.request_id = c.request_id
  LEFT JOIN fail_counts f
    ON s.experiment_group = f.experiment_group
   AND s.product = f.product
   AND s.ad_format = f.ad_format
   AND s.user_pseudo_id = f.user_pseudo_id
   AND s.request_id = f.request_id
)

SELECT
  product,
  ad_format,
  experiment_group,
  country,
  max_unit_id,
  'network' AS cnt_type,
  CAST(network_cnt AS INT64) AS cnt_value,
  CAST(success_rank AS INT64) AS success_rank,
  success_network_type,
  COUNT(*) AS request_pv
FROM success_request_facts
GROUP BY product, ad_format, experiment_group, country, max_unit_id, cnt_type, cnt_value, success_rank, success_network_type

UNION ALL

SELECT
  product,
  ad_format,
  experiment_group,
  country,
  max_unit_id,
  'placement' AS cnt_type,
  CAST(placement_cnt AS INT64) AS cnt_value,
  CAST(success_rank AS INT64) AS success_rank,
  success_network_type,
  COUNT(*) AS request_pv
FROM success_request_facts
GROUP BY product, ad_format, experiment_group, country, max_unit_id, cnt_type, cnt_value, success_rank, success_network_type

ORDER BY product, ad_format, experiment_group, country, max_unit_id, cnt_type, cnt_value, success_rank, success_network_type;
