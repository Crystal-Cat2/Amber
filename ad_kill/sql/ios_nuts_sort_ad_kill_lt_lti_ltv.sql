-- ad_kill 实验：iOS Nuts Sort LT / LTI / LTV
-- 横轴：LT day (LT0 ~ LT30)，累计值不因中间天无数据而断档
-- 只算新用户（first_open 在 2/2 ~ 3/8，确保所有用户都能观察到 LT30）
-- 分组：game_model 2a(A) / 2b(B)
-- 注意：first_open 事件不携带 game_model，需从 user_engagement 获取分组
-- MAX 表用 user_id + install_date 范围框定

-- 1a. 从 user_engagement 获取用户的 game_model 分组 + user_id
--     first_open 事件不携带 game_model 和 user_id，需从后续事件获取
WITH user_ab AS (
  SELECT
    user_pseudo_id,
    MAX(user_id) AS user_id,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)2b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort`
  WHERE event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND event_name = 'user_engagement'
    AND (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
  HAVING ab_group IS NOT NULL
),

-- 1b. 用 first_open 确定 install_date，从 user_ab 拿分组和 user_id
new_users AS (
  SELECT
    fo.user_pseudo_id,
    ua.user_id,
    MIN(DATE(TIMESTAMP_MICROS(fo.event_timestamp), 'UTC')) AS install_date,
    ua.ab_group
  FROM `transferred.hudi_ods.ios_nuts_sort` AS fo
  INNER JOIN user_ab AS ua ON fo.user_pseudo_id = ua.user_pseudo_id
  WHERE fo.event_date BETWEEN '2026-02-01' AND '2026-03-08'
    AND fo.event_name = 'first_open'
  GROUP BY fo.user_pseudo_id, ua.user_id, ua.ab_group
  HAVING install_date BETWEEN DATE '2026-02-02' AND DATE '2026-03-08'
),

-- 新用户总数
new_user_count AS (
  SELECT ab_group, COUNT(DISTINCT user_pseudo_id) AS total_new_users
  FROM new_users
  GROUP BY ab_group
),

-- 2. LT：用户每日活跃，分组从 new_users 获取
lt_raw AS (
  SELECT
    nu.ab_group,
    e.user_pseudo_id,
    DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) AS lt_day
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name = 'user_engagement'
),
lt_summary AS (
  SELECT ab_group, lt_day, COUNT(DISTINCT user_pseudo_id) AS retained_users
  FROM lt_raw
  WHERE lt_day BETWEEN 0 AND 30
  GROUP BY ab_group, lt_day
),

-- 3. Hudi 展示量：分组从 new_users 获取
hudi_imp_raw AS (
  SELECT
    nu.ab_group,
    DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) AS lt_day,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      WHEN e.event_name LIKE 'banner_%' THEN 'banner'
    END AS ad_format,
    COUNT(*) AS imp_count
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name IN ('interstitial_ad_impression', 'reward_ad_impression', 'banner_ad_impression')
  GROUP BY ab_group, lt_day, ad_format
),

-- 4. Hudi 收入：分组从 new_users 获取
hudi_rev_raw AS (
  SELECT
    nu.ab_group,
    DATE_DIFF(DATE(TIMESTAMP_MICROS(e.event_timestamp), 'UTC'), nu.install_date, DAY) AS lt_day,
    CASE
      WHEN e.event_name LIKE 'interstitial_%' THEN 'interstitial'
      WHEN e.event_name LIKE 'reward_%' THEN 'rewarded'
      WHEN e.event_name LIKE 'banner_%' THEN 'banner'
    END AS ad_format,
    SUM(
      COALESCE(
        SAFE_CAST((SELECT ep.value.int_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'revenue' LIMIT 1) AS FLOAT64),
        SAFE_CAST((SELECT ep.value.double_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'revenue' LIMIT 1) AS FLOAT64),
        SAFE_CAST((SELECT ep.value.float_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'revenue' LIMIT 1) AS FLOAT64),
        SAFE_CAST((SELECT ep.value.string_value FROM UNNEST(event_params.array) AS ep WHERE ep.key = 'revenue' LIMIT 1) AS FLOAT64)
      ) / 1000000.0
    ) AS revenue_usd
  FROM `transferred.hudi_ods.ios_nuts_sort` AS e
  INNER JOIN new_users AS nu ON e.user_pseudo_id = nu.user_pseudo_id
  WHERE e.event_date BETWEEN '2026-02-01' AND '2026-04-07'
    AND e.event_name IN ('interstitial_ad_paid', 'reward_ad_paid', 'banner_ad_paid')
  GROUP BY ab_group, lt_day, ad_format
),

-- 5. MAX：用 user_id + 时间范围框定，分格式
max_raw AS (
  SELECT
    nu.ab_group,
    DATE_DIFF(DATE(m.`Date`, 'UTC'), nu.install_date, DAY) AS lt_day,
    CASE UPPER(m.Ad_Format)
      WHEN 'INTER' THEN 'interstitial'
      WHEN 'REWARD' THEN 'rewarded'
      WHEN 'BANNER' THEN 'banner'
      ELSE LOWER(m.Ad_Format)
    END AS ad_format,
    COUNT(*) AS max_imp_count,
    SUM(m.Revenue) AS max_revenue_usd
  FROM `gpdata-224001.applovin_max.ios_nuts_sort_*` AS m
  INNER JOIN new_users AS nu
    ON m.User_ID = nu.user_id
   AND DATE(m.`Date`, 'UTC') BETWEEN nu.install_date AND DATE '2026-04-07'
  WHERE _TABLE_SUFFIX BETWEEN '20260202' AND '20260407'
    AND m.User_ID IS NOT NULL
  GROUP BY nu.ab_group, lt_day, ad_format
),

-- 6. 生成完整 LT 序列 × ab_group × ad_format，防止累计断档
ab_groups AS (SELECT 'A' AS ab_group UNION ALL SELECT 'B'),
ad_formats AS (SELECT 'interstitial' AS ad_format UNION ALL SELECT 'rewarded' UNION ALL SELECT 'banner'),
lt_days AS (SELECT lt_day FROM UNNEST(GENERATE_ARRAY(0, 30)) AS lt_day),
skeleton AS (
  SELECT a.ab_group, d.lt_day, f.ad_format
  FROM ab_groups a CROSS JOIN lt_days d CROSS JOIN ad_formats f
),

-- 7. 合并到 skeleton 上
merged AS (
  SELECT
    s.ab_group,
    s.lt_day,
    s.ad_format,
    COALESCE(hi.imp_count, 0) AS hudi_imp,
    COALESCE(hr.revenue_usd, 0) AS hudi_rev,
    COALESCE(mx.max_imp_count, 0) AS max_imp,
    COALESCE(mx.max_revenue_usd, 0) AS max_rev
  FROM skeleton AS s
  LEFT JOIN hudi_imp_raw AS hi
    ON s.ab_group = hi.ab_group AND s.lt_day = hi.lt_day AND s.ad_format = hi.ad_format
  LEFT JOIN hudi_rev_raw AS hr
    ON s.ab_group = hr.ab_group AND s.lt_day = hr.lt_day AND s.ad_format = hr.ad_format
  LEFT JOIN max_raw AS mx
    ON s.ab_group = mx.ab_group AND s.lt_day = mx.lt_day AND s.ad_format = mx.ad_format
),

-- 8. 累计
cumulative AS (
  SELECT
    ab_group, lt_day, ad_format,
    SUM(hudi_imp) OVER (PARTITION BY ab_group, ad_format ORDER BY lt_day) AS cum_hudi_imp,
    SUM(hudi_rev) OVER (PARTITION BY ab_group, ad_format ORDER BY lt_day) AS cum_hudi_rev,
    SUM(max_imp) OVER (PARTITION BY ab_group, ad_format ORDER BY lt_day) AS cum_max_imp,
    SUM(max_rev) OVER (PARTITION BY ab_group, ad_format ORDER BY lt_day) AS cum_max_rev
  FROM merged
)

-- 最终输出
SELECT
  c.ab_group,
  c.lt_day,
  lt.retained_users,
  nuc.total_new_users,
  SAFE_DIVIDE(lt.retained_users, nuc.total_new_users) AS retention_rate,
  c.ad_format,
  SAFE_DIVIDE(c.cum_hudi_imp, nuc.total_new_users) AS avg_cum_hudi_lti,
  SAFE_DIVIDE(c.cum_hudi_rev, nuc.total_new_users) AS avg_cum_hudi_ltv,
  SAFE_DIVIDE(c.cum_max_imp, nuc.total_new_users) AS avg_cum_max_lti,
  SAFE_DIVIDE(c.cum_max_rev, nuc.total_new_users) AS avg_cum_max_ltv
FROM cumulative AS c
LEFT JOIN lt_summary AS lt
  ON c.ab_group = lt.ab_group AND c.lt_day = lt.lt_day
LEFT JOIN new_user_count AS nuc
  ON c.ab_group = nuc.ab_group
ORDER BY c.ab_group, c.lt_day, c.ad_format;
