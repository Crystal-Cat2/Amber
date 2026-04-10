-- ad_kill 实验：Ball Sort Android 全用户 MAX 展示与收入（分格式分天）
-- 用户范围：实验期间所有活跃用户（不限新增）
-- 数据源：MAX 表
-- 维度：日期、AB 组、广告格式

-- 1. 从 user_engagement 获取用户的 game_model 分组 + user_id
WITH user_ab AS (
  SELECT
    user_pseudo_id,
    MAX(user_id) AS user_id,
    -- 用户首次携带 game_model 的时间戳 = 进入实验的时间
    MIN(event_timestamp) AS experiment_ts,
    CASE
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12a($|_)'
      ) THEN 'A'
      WHEN REGEXP_CONTAINS(
        (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1),
        r'(^|_)12b($|_)'
      ) THEN 'B'
    END AS ab_group
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date BETWEEN '2026-01-29' AND '2026-04-06'
    AND event_name = 'user_engagement'
    AND (SELECT up.value.string_value FROM UNNEST(user_properties.array) AS up WHERE up.key = 'game_model' LIMIT 1) IS NOT NULL
  GROUP BY user_pseudo_id, ab_group
  HAVING ab_group IS NOT NULL
)

-- 2. 关联 MAX 表，只取用户进入实验之后的数据
SELECT
  DATE(m.`Date`, 'UTC') AS event_date,
  ua.ab_group,
  CASE UPPER(m.Ad_Format)
    WHEN 'INTER' THEN 'interstitial'
    WHEN 'REWARD' THEN 'rewarded'
    WHEN 'BANNER' THEN 'banner'
    ELSE LOWER(m.Ad_Format)
  END AS ad_format,
  COUNT(*) AS impressions,
  SUM(m.Revenue) AS revenue_usd
FROM `gpdata-224001.applovin_max.ball_sort_*` AS m
INNER JOIN user_ab AS ua
  ON m.User_ID = ua.user_id
  AND TIMESTAMP(m.`Date`) >= TIMESTAMP_MICROS(ua.experiment_ts)
WHERE _TABLE_SUFFIX BETWEEN '20260130' AND '20260406'
  AND m.User_ID IS NOT NULL
GROUP BY event_date, ua.ab_group, ad_format
ORDER BY event_date, ua.ab_group, ad_format;
