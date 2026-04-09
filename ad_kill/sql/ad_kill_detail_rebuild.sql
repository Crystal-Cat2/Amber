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
