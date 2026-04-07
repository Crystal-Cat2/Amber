-- 查询目的：
-- 1. 检查倒球（ball_sort）在 2026-04-01 的 first_open 事件里，game_model 参数是否存在“key 存在但值为 NULL”的情况。
-- 2. 值为 NULL 的口径定义为：game_model 对应 value 的 string_value / int_value / double_value / float_value 均为 NULL。
-- 3. 同时输出 key 存在事件数与占比，避免“值为 NULL”分母歧义。

-- 步骤1：限定 2026-04-01 当天的 first_open 事件
WITH first_open_events AS (
  SELECT
    event_timestamp,
    event_params
  FROM `transferred.hudi_ods.ball_sort`
  WHERE event_date = '2026-04-01'
    AND event_name = 'first_open'
),

-- 步骤2：按事件判断 game_model 是否存在，以及是否存在“值全为 NULL”的参数槽位
event_level_flags AS (
  SELECT
    event_timestamp,
    EXISTS (
      SELECT 1
      FROM UNNEST(event_params.array) AS ep
      WHERE ep.key = 'game_model'
    ) AS has_game_model_key,
    EXISTS (
      SELECT 1
      FROM UNNEST(event_params.array) AS ep
      WHERE ep.key = 'game_model'
        AND ep.value.string_value IS NULL
        AND ep.value.int_value IS NULL
        AND ep.value.double_value IS NULL
        AND ep.value.float_value IS NULL
    ) AS has_game_model_null_value
  FROM first_open_events
)

-- 步骤3：汇总事件数与占比
SELECT
  COUNT(*) AS first_open_total_events,
  COUNTIF(has_game_model_key) AS game_model_key_present_events,
  COUNTIF(has_game_model_null_value) AS game_model_null_value_events,
  SAFE_DIVIDE(
    COUNTIF(has_game_model_null_value),
    COUNT(*)
  ) AS game_model_null_value_rate_in_first_open,
  SAFE_DIVIDE(
    COUNTIF(has_game_model_null_value),
    COUNTIF(has_game_model_key)
  ) AS game_model_null_value_rate_in_key_present
FROM event_level_flags;
