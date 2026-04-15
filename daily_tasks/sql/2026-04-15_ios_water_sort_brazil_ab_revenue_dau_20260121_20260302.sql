DECLARE start_date DATE DEFAULT DATE '2026-01-21';
DECLARE end_date DATE DEFAULT DATE '2026-03-02';
DECLARE target_country STRING DEFAULT 'Brazil';

-- 整体逻辑：
-- 1. 先取 Hudi 原始广告事件 raw_events。
-- 2. 基于 raw_events 为每个 user_id 归一唯一国家：出现次数最多；并列时取最近一次事件对应国家。
-- 3. 仅保留唯一国家为 Brazil 的实验事件，并据此生成活跃时间窗与 DAU。
-- 4. MAX / ULP 源表不单独筛国家，统一在归因时通过 Brazil 用户时间窗过滤。
-- 5. ULP 不再用 run_date 与 date 相差 2 天过滤，而是按给定业务键去重，保留最新 run_date。
-- 6. conf_id 使用 2026-01-21 之后新出现的 8 个实验配置，并按 user_id + ad_format 的 min/max 时间窗归因。

WITH raw_events AS (
  SELECT
    CAST(UNIX_MICROS(TIMESTAMP_MICROS(event_timestamp)) AS INT64) AS ts_us,
    LOWER(user_id) AS user_id,
    geo.country AS country_id,
    CASE
      WHEN event_name LIKE 'inter%' THEN 'INTER'
      WHEN event_name LIKE 'reward%' THEN 'REWARD'
      WHEN event_name LIKE 'banner%' THEN 'BANNER'
      ELSE NULL
    END AS ad_format,
    (
      SELECT event_param.value.string_value
      FROM UNNEST(event_params.array) AS event_param
      WHERE event_param.key = 'conf_id'
      LIMIT 1
    ) AS conf_id,
    DATE(TIMESTAMP_MICROS(event_timestamp)) AS date
  FROM `transferred.hudi_ods.ios_water_sort`
  WHERE event_date BETWEEN FORMAT_DATE('%Y-%m-%d', start_date)
    AND FORMAT_DATE('%Y-%m-%d', end_date)
    AND user_id IS NOT NULL
    AND geo.country IS NOT NULL
    AND geo.country != ''
    AND (
      event_name LIKE 'inter%'
      OR event_name LIKE 'reward%'
      OR event_name LIKE 'banner%'
    )
),

user_country AS (
  -- 从原始 raw_events 直接归一用户国家。
  SELECT
    user_id,
    ARRAY_AGG(
      country_id
      ORDER BY country_event_count DESC, last_ts_us DESC, country_id
      LIMIT 1
    )[OFFSET(0)] AS country_id
  FROM (
    SELECT
      user_id,
      country_id,
      COUNT(*) AS country_event_count,
      MAX(ts_us) AS last_ts_us
    FROM raw_events
    GROUP BY 1, 2
  )
  GROUP BY 1
),

experiment_events AS (
  SELECT
    raw_events.ts_us,
    raw_events.user_id,
    user_country.country_id,
    raw_events.ad_format,
    raw_events.conf_id,
    raw_events.date
  FROM raw_events
  JOIN user_country
    ON raw_events.user_id = user_country.user_id
  WHERE user_country.country_id = target_country
    AND raw_events.ad_format IS NOT NULL
    AND raw_events.conf_id IN (
      '4365_A', '4367_B', '4369_A', '4371_B',
      '4373_A', '4375_B'
    )
),

user_conf AS (
  SELECT
    user_id,
    ad_format,
    MIN(ts_us) AS start_us,
    MAX(ts_us) AS end_us
  FROM experiment_events
  GROUP BY 1, 2
),

max_ad AS (
  SELECT
    DATE(max_rows.`Date`) AS date,
    UNIX_MICROS(TIMESTAMP(max_rows.`Date`)) AS ts_us,
    LOWER(max_rows.User_ID) AS user_id,
    UPPER(max_rows.Ad_Format) AS ad_format,
    max_rows.Revenue AS revenue
  FROM `gpdata-224001.applovin_max.ios_water_sort_*` AS max_rows
  WHERE _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', start_date)
    AND FORMAT_DATE('%Y%m%d', end_date)
    AND max_rows.User_ID IS NOT NULL
    AND max_rows.Ad_Unit_ID IN (
      '30c24cc8f3c5ad69', '9d770005875d0d18', '9f00a7cc6022e695', '2d7c2fc696105eea',
      'ada930b30b62b8b7', '1309cff3166bfcc0', '16636b0a739da7a3', '784fb8cc8323a8c5',
      '6658d54cce4b8468', '219fe65f2e0b81df', 'e967e8824f373fc7', 'c2b17a759985ccc9',
      '667f61dca4b93114', '173b3bfba30c1740', '5439662104ddc954', 'd3e31dd65440344c',
      '31b8ee5979cc7905', '5f40037051828c32', '734de030d8644951', '4b3912822ce04335',
      '3b4751b118579caf', 'f452b7488c4db01f', '30d5b09b21cd6c07', 'a8b1e4c6e4efbf8f',
      'ba9ac6e6222c05c4', 'a1c39761caf16dd1', '2df812ea696e3795', 'dbf1aec3fc857101',
      '82fc927b61b478de', 'a2d82efb7ac16224'
    )
    AND UPPER(max_rows.Ad_Format) IN ('INTER', 'REWARD', 'BANNER')
    AND max_rows.Revenue IS NOT NULL
),

ulp_ads AS (
  SELECT
    deduped_ulp.date AS date,
    CAST(UNIX_MICROS(deduped_ulp.event_timestamp) AS INT64) AS ts_us,
    COALESCE(
      LOWER(NULLIF(deduped_ulp.advertising_vendor_id, '')),
      LOWER(NULLIF(deduped_ulp.advertising_id, ''))
    ) AS user_id,
    CASE UPPER(REPLACE(deduped_ulp.ad_unit, ' ', '_'))
      WHEN 'INTERSTITIAL' THEN 'INTER'
      WHEN 'REWARDED_VIDEO' THEN 'REWARD'
      WHEN 'BANNER' THEN 'BANNER'
      ELSE NULL
    END AS ad_format,
    deduped_ulp.revenue AS revenue
  FROM (
    SELECT
      external_ads.date,
      external_ads.event_timestamp,
      external_ads.advertising_id,
      external_ads.advertising_vendor_id,
      external_ads.ad_unit,
      external_ads.revenue,
      ROW_NUMBER() OVER (
        PARTITION BY
          external_ads.event_timestamp,
          external_ads.advertising_id,
          external_ads.advertising_vendor_id,
          external_ads.country,
          external_ads.placement,
          external_ads.mediation_ad_unit_id,
          external_ads.mediation_ad_unit_name
        ORDER BY external_ads.run_date DESC
      ) AS rm
    FROM `transferred.dw_external_data.external_ad_ironsource_data` AS external_ads
    WHERE external_ads.slim_name = 'ios_water_sort'
      AND external_ads.date BETWEEN start_date AND end_date
      AND external_ads.mediation_ad_unit_id IN (
      'uxzcprxh1nzuikvf', '3f6lizp5hnf34f2u', 'vrqzjei717qucthv',
      'bz9x7pweevaqfcom', '6xa0pvaaersz6s0x', '2x1azxd9vwy6zpfd',
      '29fg5oqi7vfcahno', '6k9hervjlpjo164f', 'hvj218oy87j7qrlq',
      'gdltpfo1h7vnvu79', 'scxxrdd35zor29lb', 'v3m8snwehjep9xpe',
      'o1mvmd4qc36ebyz9', 'zmzfeim1znp77rzw', 'yukszdbnx5hv7i1k',
      '5wk3zzxsrc7uq4og', 'zuto6ypkt0l7iuhs', 'oz7ddbwa5vt7h7m6',
      '27utiu1jr8gpb65j', 'byfx8nfv7y7f8aww', 'thwaoxyadpsaovx9',
      'odgaetkb49qclvib', 'wgtge1zs56pk9jxz', '61yibhb1iasp4dvf',
      'dpdby93t2y577iu0', 'su3qknczc0fcha9r', '709e9amiduvqbc10',
      'oaccnpd3exeakx1k', '04rdw2taealjitpf'
    )
      AND external_ads.revenue IS NOT NULL
  ) AS deduped_ulp
  WHERE deduped_ulp.rm = 1
),

revenue_stats AS (
  SELECT
    max_ad.date AS date,
    target_country AS country_id,
    'MAX' AS ab_group,
    max_ad.ad_format AS ad_format,
    COUNT(*) AS impression,
    SUM(max_ad.revenue) AS revenue
  FROM max_ad
  JOIN user_conf
    ON max_ad.user_id = user_conf.user_id
   AND max_ad.ad_format = user_conf.ad_format
   AND max_ad.ts_us BETWEEN user_conf.start_us AND user_conf.end_us
  GROUP BY 1, 2, 3, 4

  UNION ALL

  SELECT
    ulp_ads.date AS date,
    target_country AS country_id,
    'ULP' AS ab_group,
    ulp_ads.ad_format AS ad_format,
    COUNT(*) AS impression,
    SUM(ulp_ads.revenue) AS revenue
  FROM ulp_ads
  JOIN user_conf
    ON ulp_ads.user_id = user_conf.user_id
   AND ulp_ads.ad_format = user_conf.ad_format
   AND ulp_ads.ts_us BETWEEN user_conf.start_us AND user_conf.end_us
  WHERE ulp_ads.user_id IS NOT NULL
    AND ulp_ads.ad_format IS NOT NULL
  GROUP BY 1, 2, 3, 4
),

dau_stats AS (
  SELECT
    date,
    country_id,
    CASE
      WHEN conf_id LIKE '%A' THEN 'MAX'
      WHEN conf_id LIKE '%B' THEN 'ULP'
      ELSE NULL
    END AS ab_group,
    COUNT(DISTINCT user_id) AS dau
  FROM experiment_events
  GROUP BY 1, 2, 3
)

SELECT
  revenue_stats.date AS date,
  revenue_stats.country_id AS country_id,
  revenue_stats.ab_group AS ab_group,
  revenue_stats.ad_format AS ad_format,
  revenue_stats.impression AS impression,
  revenue_stats.revenue AS revenue,
  dau_stats.dau AS dau
FROM revenue_stats
LEFT JOIN dau_stats
  ON revenue_stats.date = dau_stats.date
 AND revenue_stats.country_id = dau_stats.country_id
 AND revenue_stats.ab_group = dau_stats.ab_group
ORDER BY 1, 2, 3, 4;
