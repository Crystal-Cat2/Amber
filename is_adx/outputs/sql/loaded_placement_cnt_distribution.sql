-- 查询目的：统计 latency 事件中“加载成功”的 placement 去重个数分布。
-- 输出字段：experiment_group, product, ad_format, loaded_placement_cnt, request_pv, total_request_pv, share
-- 关键口径：
-- 1. 成功定义为 status = 'AD_LOADED'。
-- 2. 在单次 request_id 内，对成功的 placement_id 做去重计数。
-- 3. 分布按 request 粒度统计，每个 loaded_placement_cnt 桶对应 request 数及占比。

-- 步骤 1：先定义符合口径的 latency 明细，并在 request 粒度统计成功 placement 去重个数
WITH per_request_loaded_placements AS (
  SELECT
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id,
    COUNT(DISTINCT IF(status = 'AD_LOADED', placement_id, NULL)) AS loaded_placement_cnt
  FROM `commercial-adx.lmh.isadx_adslog_latency_detail`
  WHERE experiment_group IN ('have_is_adx', 'no_is_adx')
    AND ad_format IN ('interstitial', 'rewarded')
    AND request_id IS NOT NULL
    AND network IS NOT NULL
    AND network != 'TpAdxCustomAdapter'
    AND placement_id IS NOT NULL
    AND placement_id != ''
    AND LOWER(COALESCE(network_type, '')) IN ('bidding', 'waterfall')
  GROUP BY
    experiment_group,
    product,
    ad_format,
    user_pseudo_id,
    request_id
)

-- 步骤 2：按成功 placement 个数分布聚合 request 数，并计算占比
SELECT
  experiment_group,
  product,
  ad_format,
  loaded_placement_cnt,
  COUNT(*) AS request_pv,
  SUM(COUNT(*)) OVER (
    PARTITION BY experiment_group, product, ad_format
  ) AS total_request_pv,
  SAFE_DIVIDE(
    COUNT(*),
    SUM(COUNT(*)) OVER (
      PARTITION BY experiment_group, product, ad_format
    )
  ) AS share
FROM per_request_loaded_placements
GROUP BY
  experiment_group,
  product,
  ad_format,
  loaded_placement_cnt
ORDER BY
  product,
  ad_format,
  experiment_group,
  loaded_placement_cnt;
