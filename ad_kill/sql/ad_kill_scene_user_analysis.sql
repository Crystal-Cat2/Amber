-- ad_kill 实验 ad_kill_scene 分析（第5关）
-- 数据源：commercial-adx.lmh.ad_kill_detail
-- 目的：以第5关去重用户为分母，各 scene 的 PV 及新老用户占比

-- 第5关全量事件（game_new_start, levelid=5）
WITH level5_events AS (
  SELECT
    product,
    user_pseudo_id,
    ab_group,
    user_type,
    (SELECT ep.value.string_value
     FROM UNNEST(event_params.array) AS ep
     WHERE ep.key = 'ad_kill_scene') AS ad_kill_scene
  FROM `commercial-adx.lmh.ad_kill_detail`
  WHERE event_name = 'game_new_start'
    AND (SELECT ep.value.int_value
         FROM UNNEST(event_params.array) AS ep
         WHERE ep.key = 'levelid') = 5
),

-- 第5关去重 DAU（分母）：按 product/ab_group
level5_dau AS (
  SELECT
    product,
    ab_group,
    COUNT(DISTINCT user_pseudo_id) AS dau
  FROM level5_events
  GROUP BY product, ab_group
),

-- 第5关去重 DAU 按 user_type
level5_dau_by_type AS (
  SELECT
    product,
    ab_group,
    user_type,
    COUNT(DISTINCT user_pseudo_id) AS dau
  FROM level5_events
  GROUP BY product, ab_group, user_type
),

-- 每个 scene 的 PV（事件次数）和 UV（去重用户）
scene_pv AS (
  SELECT
    product,
    ab_group,
    ad_kill_scene,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_pseudo_id) AS uv
  FROM level5_events
  WHERE ad_kill_scene IS NOT NULL
  GROUP BY product, ab_group, ad_kill_scene
),

-- 每个 scene 按新老用户的 PV 和 UV
scene_by_type AS (
  SELECT
    product,
    ab_group,
    ad_kill_scene,
    user_type,
    COUNT(*) AS pv,
    COUNT(DISTINCT user_pseudo_id) AS uv
  FROM level5_events
  WHERE ad_kill_scene IS NOT NULL
  GROUP BY product, ab_group, ad_kill_scene, user_type
)

-- Part 1: 整体（不分新老）—— scene PV/UV 占第5关 DAU 的比例
SELECT
  'overall' AS view_type,
  s.product,
  s.ab_group,
  CAST(NULL AS STRING) AS user_type,
  s.ad_kill_scene,
  d.dau AS level5_dau,
  s.pv,
  s.uv,
  ROUND(SAFE_DIVIDE(s.uv, d.dau), 4) AS uv_ratio
FROM scene_pv s
JOIN level5_dau d
  ON s.product = d.product AND s.ab_group = d.ab_group

UNION ALL

-- Part 2: 按新老用户 —— 每个 scene 内新老用户 PV/UV
SELECT
  'by_user_type' AS view_type,
  st.product,
  st.ab_group,
  st.user_type,
  st.ad_kill_scene,
  dt.dau AS level5_dau,
  st.pv,
  st.uv,
  ROUND(SAFE_DIVIDE(st.uv, dt.dau), 4) AS uv_ratio
FROM scene_by_type st
JOIN level5_dau_by_type dt
  ON st.product = dt.product
  AND st.ab_group = dt.ab_group
  AND st.user_type = dt.user_type

ORDER BY product, ad_kill_scene, view_type, ab_group, user_type;
