# Facebook latency 对比报告

## 第三部分：Facebook 对比

### 结论
- 这一部分复用 AdMob 第三部分的状态码口径，只把渠道从 `AdMob` 替换成 `Facebook`。
- 数据按时间窗内全版本汇总，不再限制版本，也不再输出版本维度。
- `facebook_started_cnt` 统计 Facebook 渠道中状态为 `-2/-3` 的数量。
- `facebook_total_minus_not_started_cnt` 统计“全部 latency 总数减去 Facebook 渠道状态为 `-1` 的数量”。

### facebook_started_cnt vs Facebook Bidding requests

#### 结论
- `facebook_started_cnt` 相对 `Facebook Bidding requests` 的恢复比例在 0.15% 到 6.25% 之间。
- 最接近后台的点是 `ios_screw_puzzle/interstitial`，比例为 0.15%；偏差最大的点是 `screw_puzzle/banner`，比例为 6.25%。

#### 数据

| product | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | banner | 68,066,139 | 254,394 | 67,811,745 | 0.37% |
| ios_screw_puzzle | interstitial | 27,070,424 | 39,382 | 27,031,042 | 0.15% |
| ios_screw_puzzle | rewarded | 15,803,351 | 38,015 | 15,765,336 | 0.24% |
| screw_puzzle | banner | 125,538,708 | 7,845,690 | 117,693,018 | 6.25% |
| screw_puzzle | interstitial | 41,492,266 | 1,530,720 | 39,961,546 | 3.69% |
| screw_puzzle | rewarded | 18,795,271 | 850,452 | 17,944,819 | 4.52% |

### facebook_total_minus_not_started_cnt vs Facebook Bidding requests

#### 结论
- `facebook_total_minus_not_started_cnt` 相对 `Facebook Bidding requests` 的恢复比例在 72.42% 到 134.72% 之间。
- 最接近后台的点是 `ios_screw_puzzle/rewarded`，比例为 72.42%；偏差最大的点是 `ios_screw_puzzle/banner`，比例为 134.72%。

#### 数据

| product | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | banner | 68,066,139 | 91,695,783 | -23,629,644 | 134.72% |
| ios_screw_puzzle | interstitial | 27,070,424 | 25,688,607 | 1,381,817 | 94.90% |
| ios_screw_puzzle | rewarded | 15,803,351 | 11,445,526 | 4,357,825 | 72.42% |
| screw_puzzle | banner | 125,538,708 | 130,059,719 | -4,521,011 | 103.60% |
| screw_puzzle | interstitial | 41,492,266 | 43,201,406 | -1,709,140 | 104.12% |
| screw_puzzle | rewarded | 18,795,271 | 17,354,116 | 1,441,155 | 92.33% |

### facebook_started_cnt vs Facebook Requests

#### 结论
- `facebook_started_cnt` 相对 `Facebook Requests` 的恢复比例在 39.26% 到 83.79% 之间。
- 最接近后台的点是 `ios_screw_puzzle/rewarded`，比例为 39.26%；偏差最大的点是 `screw_puzzle/banner`，比例为 83.79%。

#### 数据

| product | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | banner | 316,994 | 254,394 | 62,600 | 80.25% |
| ios_screw_puzzle | interstitial | 70,976 | 39,382 | 31,594 | 55.49% |
| ios_screw_puzzle | rewarded | 96,821 | 38,015 | 58,806 | 39.26% |
| screw_puzzle | banner | 9,363,049 | 7,845,690 | 1,517,359 | 83.79% |
| screw_puzzle | interstitial | 1,841,197 | 1,530,720 | 310,477 | 83.14% |
| screw_puzzle | rewarded | 1,110,220 | 850,452 | 259,768 | 76.60% |

### facebook_total_minus_not_started_cnt vs Facebook Requests

#### 结论
- `facebook_total_minus_not_started_cnt` 相对 `Facebook Requests` 的恢复比例在 1389.07% 到 36193.37% 之间。
- 最接近后台的点是 `screw_puzzle/banner`，比例为 1389.07%；偏差最大的点是 `ios_screw_puzzle/interstitial`，比例为 36193.37%。

#### 数据

| product | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | banner | 316,994 | 91,695,783 | -91,378,789 | 28926.66% |
| ios_screw_puzzle | interstitial | 70,976 | 25,688,607 | -25,617,631 | 36193.37% |
| ios_screw_puzzle | rewarded | 96,821 | 11,445,526 | -11,348,705 | 11821.33% |
| screw_puzzle | banner | 9,363,049 | 130,059,719 | -120,696,670 | 1389.07% |
| screw_puzzle | interstitial | 1,841,197 | 43,201,406 | -41,360,209 | 2346.38% |
| screw_puzzle | rewarded | 1,110,220 | 17,354,116 | -16,243,896 | 1563.12% |

### 指标说明
> - `facebook_started_cnt`：Facebook 渠道中 `fill_status_code` 为 `-2/-3` 的数量。
> - `facebook_total_minus_not_started_cnt`：全部 latency 事件总数减去 Facebook 渠道中 `fill_status_code = -1` 的数量。
> - `Bidding requests` 与 `Requests`：来自 `facebook.csv`，按 2026-01-05 到 2026-01-12 聚合。

## SQL 附录

```sql
-- 查询目的：沿用 admob_latency 第三部分的数据源与状态码口径，只把渠道从 AdMob 改成 Facebook，
-- 且不再保留网络状态拆分，只输出 Facebook 渠道的两套核心对比指标。
WITH
-- 全量 latency 基表：继续使用原第三部分的时间窗和 adslog_load_latency 事件，
-- 但不再限制 app 版本，也不输出版本维度。
all_latency_base AS (
  SELECT
    'screw_puzzle' AS product,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_load_latency'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name = 'adslog_load_latency'
),

-- 全量 latency 总数：这里的“总数”是所有渠道的总数，不限制 Facebook。
all_latency_counts AS (
  SELECT
    product,
    ad_format,
    COUNT(*) AS latency_total_cnt
  FROM all_latency_base
  WHERE ad_format IN ('banner', 'interstitial', 'rewarded')
  GROUP BY
    product,
    ad_format
),

-- Facebook 渠道明细：继续沿用原第三部分的 event_params 展开方式，只把渠道前缀改成 Facebook。
facebook_rows AS (
  SELECT
    b.product,
    b.ad_format,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(2)] AS fill_status_code
  FROM all_latency_base b,
  UNNEST(b.event_params) AS ep
  WHERE STARTS_WITH(ep.key, 'Facebook_')
    AND b.ad_format IN ('banner', 'interstitial', 'rewarded')
),

-- Facebook 的 started 口径：只统计 status = -2 / -3。
facebook_started_counts AS (
  SELECT
    product,
    ad_format,
    COUNTIF(fill_status_code IN ('-2', '-3')) AS facebook_started_cnt
  FROM facebook_rows
  GROUP BY
    product,
    ad_format
),

-- Facebook 的 not_started 口径：只统计 status = -1，后续用“全部总数 - not_started”。
facebook_not_started_counts AS (
  SELECT
    product,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS facebook_not_started_cnt
  FROM facebook_rows
  GROUP BY
    product,
    ad_format
),

-- 最终对比基表：started 仍是 Facebook 渠道内的 -2 / -3，
-- total_minus_not_started 则是“全部 latency 总数 - Facebook 的 -1 数量”。
facebook_compare_base AS (
  SELECT
    t.product,
    t.ad_format,
    COALESCE(s.facebook_started_cnt, 0) AS facebook_started_cnt,
    t.latency_total_cnt - COALESCE(n.facebook_not_started_cnt, 0) AS facebook_total_minus_not_started_cnt
  FROM all_latency_counts t
  LEFT JOIN facebook_started_counts s
    ON t.product = s.product
   AND t.ad_format = s.ad_format
  LEFT JOIN facebook_not_started_counts n
    ON t.product = n.product
   AND t.ad_format = n.ad_format
)

SELECT
  product,
  ad_format,
  facebook_started_cnt,
  facebook_total_minus_not_started_cnt
FROM facebook_compare_base
ORDER BY
  product,
  ad_format
```
