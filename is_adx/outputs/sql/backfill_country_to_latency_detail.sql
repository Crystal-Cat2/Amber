-- 查询目的：给 commercial-adx.lmh.isadx_adslog_latency_detail 新增并回填 country 列。
-- 关键口径：
-- 1. country 来源于与目标表同时间窗内的 Hudi 高覆盖事件的非空 geo.country。
-- 2. 用户国家按 product + user_pseudo_id 选最高频国家；并列时取最近时间、再按国家码字典序打平。
-- 3. 通过 product + user_pseudo_id JOIN 回填到 latency 明细表。

CREATE TEMP TABLE user_country_map AS
WITH country_events AS (
  SELECT
    'com.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    geo.country AS country,
    event_timestamp
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-13'
    AND event_name IN (
      'universal_alive',
      'screen_view',
      'user_engagement',
      'app_open',
      'session_start',
      'loading_pv',
      'iap_initialized',
      'lib_mediation_initialize',
      'banner_ad_request',
      'interstitial_ad_request',
      'reward_ad_request',
      'loading_done',
      'home_pv',
      'game_new_start'
    )
    AND geo.country IS NOT NULL
    AND geo.country != ''

  UNION ALL

  SELECT
    'ios.takeoffbolts.screw.puzzle' AS product,
    user_pseudo_id,
    geo.country AS country,
    event_timestamp
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-13'
    AND event_name IN (
      'universal_alive',
      'screen_view',
      'user_engagement',
      'app_open',
      'session_start',
      'loading_pv',
      'iap_initialized',
      'lib_mediation_initialize',
      'banner_ad_request',
      'interstitial_ad_request',
      'reward_ad_request',
      'loading_done',
      'home_pv',
      'game_new_start'
    )
    AND geo.country IS NOT NULL
    AND geo.country != ''
),
country_frequency AS (
  SELECT
    product,
    user_pseudo_id,
    country,
    COUNT(*) AS country_event_cnt,
    MAX(event_timestamp) AS latest_event_timestamp
  FROM country_events
  WHERE user_pseudo_id IS NOT NULL
  GROUP BY
    product,
    user_pseudo_id,
    country
),
ranked_country AS (
  SELECT
    product,
    user_pseudo_id,
    country,
    ROW_NUMBER() OVER (
      PARTITION BY product, user_pseudo_id
      ORDER BY country_event_cnt DESC, latest_event_timestamp DESC, country ASC
    ) AS country_rank
  FROM country_frequency
)
SELECT
  product,
  user_pseudo_id,
  country
FROM ranked_country
WHERE country_rank = 1;

ALTER TABLE `commercial-adx.lmh.isadx_adslog_latency_detail`
ADD COLUMN IF NOT EXISTS country STRING;

UPDATE `commercial-adx.lmh.isadx_adslog_latency_detail` AS latency
SET country = map.country
FROM user_country_map AS map
WHERE latency.country IS NULL
  AND latency.product = map.product
  AND latency.user_pseudo_id = map.user_pseudo_id;

SELECT
  product,
  COUNT(*) AS total_event_cnt,
  COUNTIF(country IS NOT NULL) AS filled_country_event_cnt,
  COUNTIF(country IS NULL) AS null_country_event_cnt,
  SAFE_DIVIDE(COUNTIF(country IS NOT NULL), COUNT(*)) AS filled_country_ratio
FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
GROUP BY product
ORDER BY product;
