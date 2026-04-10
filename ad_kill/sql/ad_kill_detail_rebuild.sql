-- 重建 ad_kill_detail 明细表（优化版）
-- 优化点：
--   1. 提前从 user_engagement 提取 user→ab_group 映射，避免全量逐行 REGEXP
--   2. 过滤 event_name，只保留分析所需事件，去掉 SDK/归因/eCPM 分层等无用事件
--   3. INNER JOIN user_ab 提前淘汰无分组用户，减少后续 CTE 数据量
-- 步骤：
--   1. user_ab — 从 user_engagement 提取 user_pseudo_id → ab_group
--   2. base_events — 过滤事件 + JOIN user_ab
--   3. new_users — 从 base_events 提取 first_open 新用户
--   4. user_country — 按事件数取用户唯一国家
--   5. 最终 JOIN，标记新老用户，替换为唯一国家

CREATE OR REPLACE TABLE `commercial-adx.lmh.ad_kill_detail` AS

-- 步骤1: 从 user_engagement 提取 AB 分组映射（REGEXP 只执行一次）
WITH user_ab AS (
  SELECT user_pseudo_id, 'ball_sort' AS product, ab_group
  FROM (
    SELECT
      user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(12a)([^a-z0-9]|$)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(12b)([^a-z0-9]|$)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ball_sort`
    WHERE event_date BETWEEN '2026-01-30' AND '2026-04-07'
      AND event_name = 'user_engagement'
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE ab_group IS NOT NULL
  GROUP BY user_pseudo_id, ab_group

  UNION ALL

  SELECT user_pseudo_id, 'ios_nuts_sort' AS product, ab_group
  FROM (
    SELECT
      user_pseudo_id,
      CASE
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(2a)([^a-z0-9]|$)'
        ) THEN 'A'
        WHEN REGEXP_CONTAINS(
          LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
          r'(^|[^a-z0-9])(2b)([^a-z0-9]|$)'
        ) THEN 'B'
      END AS ab_group
    FROM `transferred.hudi_ods.ios_nuts_sort`
    WHERE event_date BETWEEN '2026-02-02' AND '2026-04-07'
      AND event_name = 'user_engagement'
      AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
  )
  WHERE ab_group IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
),

-- 步骤2: 过滤事件 + INNER JOIN user_ab（提前淘汰无分组用户）
base_events AS (
  SELECT
    'ball_sort' AS product,
    DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') AS event_date,
    e.event_timestamp,
    e.user_pseudo_id,
    LOWER(e.user_id) AS user_id,
    e.event_name,
    e.event_params,
    e.user_properties,
    e.geo.country AS country,
    ua.ab_group
  FROM `transferred.hudi_ods.ball_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ball_sort'
  WHERE e.event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND e.user_pseudo_id IS NOT NULL AND e.user_pseudo_id != ''
    AND (
      e.event_name IN (
        'first_open', 'user_engagement', 'universal_alive', 'session_start',
        'app_open', 'app_quit',
        'game_new_start', 'game_win', 'game_restart', 'game_continue',
        'all_color_finish', 'same_color_finish',
        'item_click', 'item_action',
        'lib_fullscreen_ad_killed'
      )
      OR e.event_name LIKE 'interstitial_ad_%'
      OR e.event_name LIKE 'reward_ad_%'
      OR e.event_name LIKE 'banner_ad_%'
    )

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC') AS event_date,
    e.event_timestamp,
    e.user_pseudo_id,
    LOWER(e.user_id) AS user_id,
    e.event_name,
    e.event_params,
    e.user_properties,
    e.geo.country AS country,
    ua.ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort` e
  INNER JOIN user_ab ua
    ON e.user_pseudo_id = ua.user_pseudo_id AND ua.product = 'ios_nuts_sort'
  WHERE e.event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND e.user_pseudo_id IS NOT NULL AND e.user_pseudo_id != ''
    AND (
      e.event_name IN (
        'first_open', 'user_engagement', 'universal_alive', 'session_start',
        'app_open', 'app_quit',
        'game_new_start', 'game_win', 'game_restart', 'game_continue',
        'all_color_finish', 'same_color_finish',
        'item_click', 'item_action',
        'lib_fullscreen_ad_killed'
      )
      OR e.event_name LIKE 'interstitial_ad_%'
      OR e.event_name LIKE 'reward_ad_%'
      OR e.event_name LIKE 'banner_ad_%'
    )
),

-- 步骤3: 新用户（从 base_events 提取 first_open，不再单独扫描源表）
new_users AS (
  SELECT DISTINCT product, user_pseudo_id
  FROM base_events
  WHERE event_name = 'first_open'
    AND (
      (product = 'ball_sort' AND event_date BETWEEN '2026-01-30' AND '2026-03-08')
      OR (product = 'ios_nuts_sort' AND event_date BETWEEN '2026-02-02' AND '2026-03-08')
    )
),

-- 步骤4: 按事件数取用户唯一国家
user_country AS (
  SELECT
    product,
    user_pseudo_id,
    country AS unique_country
  FROM (
    SELECT
      product,
      user_pseudo_id,
      country,
      COUNT(*) AS event_cnt,
      ROW_NUMBER() OVER (
        PARTITION BY product, user_pseudo_id
        ORDER BY COUNT(*) DESC
      ) AS rn
    FROM base_events
    WHERE country IS NOT NULL AND country != ''
    GROUP BY product, user_pseudo_id, country
  )
  WHERE rn = 1
)

-- 步骤5: 最终输出，JOIN 新用户标记 + 唯一国家
SELECT
  b.product,
  b.event_date,
  b.event_timestamp,
  b.user_pseudo_id,
  b.user_id,
  b.event_name,
  b.event_params,
  b.user_properties,
  uc.unique_country AS country,
  b.ab_group,
  CASE WHEN n.user_pseudo_id IS NOT NULL THEN 'new' ELSE 'old' END AS user_type
FROM base_events b
LEFT JOIN new_users n
  ON b.product = n.product
 AND b.user_pseudo_id = n.user_pseudo_id
LEFT JOIN user_country uc
  ON b.product = uc.product
 AND b.user_pseudo_id = uc.user_pseudo_id;
