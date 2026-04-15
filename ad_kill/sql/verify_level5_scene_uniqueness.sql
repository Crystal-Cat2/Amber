-- 第5关 ad_kill_scene 用户级唯一性核对
-- 目的：
-- 1. 确认同一用户在第5关是否会出现多个 scene 值
-- 2. 确认多值组合是否主要是 none+long / none+short
-- 3. 确认 long_watch_kill / short_watch_repeat_kill 对单个用户是否只出现一次

WITH level5_events AS (
  SELECT
    'ball_sort' AS product,
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
    END AS ab_group,
    event_name,
    event_timestamp,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'none'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''

  UNION ALL

  SELECT
    'ios_nuts_sort' AS product,
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
    END AS ab_group,
    event_name,
    event_timestamp,
    COALESCE(
      (SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'ad_kill_scene'),
      'none'
    ) AS ad_kill_scene
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-07'
    AND event_name IN ('game_new_start', 'game_win')
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'levelid') = 5
    AND (SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'activity_id') = 0
    AND user_pseudo_id IS NOT NULL
    AND user_pseudo_id != ''
),

ab_level5_events AS (
  SELECT *
  FROM level5_events
  WHERE ab_group IS NOT NULL
),

user_scene_stats AS (
  SELECT
    product,
    user_pseudo_id,
    COUNT(*) AS total_level5_events,
    COUNTIF(ad_kill_scene = 'none') AS none_event_cnt,
    COUNTIF(ad_kill_scene = 'long_watch_kill') AS long_event_cnt,
    COUNTIF(ad_kill_scene = 'short_watch_repeat_kill') AS short_event_cnt,
    COUNT(DISTINCT ad_kill_scene) AS distinct_scene_cnt,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT ad_kill_scene ORDER BY ad_kill_scene), '|') AS scene_combo
  FROM ab_level5_events
  GROUP BY product, user_pseudo_id
),

combo_summary AS (
  SELECT
    product,
    scene_combo,
    COUNT(*) AS user_cnt
  FROM user_scene_stats
  GROUP BY product, scene_combo
),

product_summary AS (
  SELECT
    product,
    COUNT(*) AS total_users,
    COUNTIF(distinct_scene_cnt > 1) AS multi_scene_users,
    COUNTIF(scene_combo = 'long_watch_kill|none') AS none_long_users,
    COUNTIF(scene_combo = 'none|short_watch_repeat_kill') AS none_short_users,
    COUNTIF(scene_combo = 'long_watch_kill|none|short_watch_repeat_kill') AS none_long_short_users,
    COUNTIF(scene_combo = 'long_watch_kill|short_watch_repeat_kill') AS long_short_users,
    COUNTIF(long_event_cnt > 1) AS users_long_more_than_once,
    MAX(long_event_cnt) AS max_long_event_cnt,
    COUNTIF(short_event_cnt > 1) AS users_short_more_than_once,
    MAX(short_event_cnt) AS max_short_event_cnt
  FROM user_scene_stats
  GROUP BY product
)

SELECT
  'product_summary' AS section,
  product,
  CAST(NULL AS STRING) AS key1,
  CAST(NULL AS STRING) AS key2,
  total_users AS value1,
  multi_scene_users AS value2,
  none_long_users AS value3,
  none_short_users AS value4,
  none_long_short_users AS value5,
  long_short_users AS value6,
  users_long_more_than_once AS value7,
  max_long_event_cnt AS value8,
  users_short_more_than_once AS value9,
  max_short_event_cnt AS value10
FROM product_summary

UNION ALL

SELECT
  'top_combo' AS section,
  product,
  scene_combo AS key1,
  CAST(NULL AS STRING) AS key2,
  user_cnt AS value1,
  CAST(NULL AS INT64) AS value2,
  CAST(NULL AS INT64) AS value3,
  CAST(NULL AS INT64) AS value4,
  CAST(NULL AS INT64) AS value5,
  CAST(NULL AS INT64) AS value6,
  CAST(NULL AS INT64) AS value7,
  CAST(NULL AS INT64) AS value8,
  CAST(NULL AS INT64) AS value9,
  CAST(NULL AS INT64) AS value10
FROM (
  SELECT
    product,
    scene_combo,
    user_cnt,
    ROW_NUMBER() OVER (PARTITION BY product ORDER BY user_cnt DESC, scene_combo) AS rn
  FROM combo_summary
)
WHERE rn <= 10

UNION ALL

SELECT
  'long_gt1_example' AS section,
  product,
  user_pseudo_id AS key1,
  scene_combo AS key2,
  long_event_cnt AS value1,
  total_level5_events AS value2,
  CAST(NULL AS INT64) AS value3,
  CAST(NULL AS INT64) AS value4,
  CAST(NULL AS INT64) AS value5,
  CAST(NULL AS INT64) AS value6,
  CAST(NULL AS INT64) AS value7,
  CAST(NULL AS INT64) AS value8,
  CAST(NULL AS INT64) AS value9,
  CAST(NULL AS INT64) AS value10
FROM (
  SELECT
    product,
    user_pseudo_id,
    scene_combo,
    long_event_cnt,
    total_level5_events,
    ROW_NUMBER() OVER (PARTITION BY product ORDER BY long_event_cnt DESC, total_level5_events DESC, user_pseudo_id) AS rn
  FROM user_scene_stats
  WHERE long_event_cnt > 1
)
WHERE rn <= 5

UNION ALL

SELECT
  'short_gt1_example' AS section,
  product,
  user_pseudo_id AS key1,
  scene_combo AS key2,
  short_event_cnt AS value1,
  total_level5_events AS value2,
  CAST(NULL AS INT64) AS value3,
  CAST(NULL AS INT64) AS value4,
  CAST(NULL AS INT64) AS value5,
  CAST(NULL AS INT64) AS value6,
  CAST(NULL AS INT64) AS value7,
  CAST(NULL AS INT64) AS value8,
  CAST(NULL AS INT64) AS value9,
  CAST(NULL AS INT64) AS value10
FROM (
  SELECT
    product,
    user_pseudo_id,
    scene_combo,
    short_event_cnt,
    total_level5_events,
    ROW_NUMBER() OVER (PARTITION BY product ORDER BY short_event_cnt DESC, total_level5_events DESC, user_pseudo_id) AS rn
  FROM user_scene_stats
  WHERE short_event_cnt > 1
)
WHERE rn <= 5

ORDER BY section, product, value1 DESC;
