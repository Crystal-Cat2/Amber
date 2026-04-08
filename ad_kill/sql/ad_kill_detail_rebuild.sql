-- 重建 ad_kill_detail 明细表
-- 步骤1: new_users — 各产品新用户入组窗口
-- 步骤2: base_events — 全量事件（入组起始 ~ 观察期结束 2026-04-07）
-- 步骤3: user_country — 按事件数取用户唯一国家
-- 步骤4: 最终 JOIN，标记新老用户，替换为唯一国家

CREATE OR REPLACE TABLE `commercial-adx.lmh.ad_kill_detail` AS

-- 步骤1: 各产品新用户（first_open）
WITH new_users AS (
  SELECT DISTINCT
    'ball_sort' AS product,
    user_pseudo_id
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_name = 'first_open'
    AND event_date BETWEEN '2026-01-30' AND '2026-03-08'
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''

  UNION DISTINCT

  SELECT DISTINCT
    'ios_nuts_sort' AS product,
    user_pseudo_id
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_name = 'first_open'
    AND event_date BETWEEN '2026-02-02' AND '2026-03-08'
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
),

-- 步骤2: 全量事件，去掉 user_id 非空过滤，避免丢用户
base_events AS (
  SELECT
    'ball_sort' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    event_timestamp,
    user_pseudo_id,
    LOWER(user_id) AS user_id,
    event_name,
    event_params,
    user_properties,
    geo.country AS country,
    CASE
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(12a)([^a-z0-9]|$)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(12b)([^a-z0-9]|$)'
      ) THEN 'B'
      ELSE NULL
    END AS ab_group
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-30' AND '2026-04-07'
    AND event_name IS NOT NULL AND event_name != ''
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
    DATE(TIMESTAMP_MICROS(event_timestamp), 'UTC') AS event_date,
    event_timestamp,
    user_pseudo_id,
    LOWER(user_id) AS user_id,
    event_name,
    event_params,
    user_properties,
    geo.country AS country,
    CASE
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(2a)([^a-z0-9]|$)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        LOWER((SELECT value.string_value FROM UNNEST(user_properties.array) WHERE key = 'game_model')),
        r'(^|[^a-z0-9])(2b)([^a-z0-9]|$)'
      ) THEN 'B'
      ELSE NULL
    END AS ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-02-02' AND '2026-04-07'
    AND event_name IS NOT NULL AND event_name != ''
    AND user_pseudo_id IS NOT NULL AND user_pseudo_id != ''
),

-- 步骤3: 按事件数取用户唯一国家
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

-- 步骤4: 最终输出，JOIN 新用户标记 + 唯一国家
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
 AND b.user_pseudo_id = uc.user_pseudo_id
WHERE b.ab_group IS NOT NULL;
