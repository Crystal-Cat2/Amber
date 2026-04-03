# AdMob latency 排查报告

## 第一部分：主版本覆盖

### 结论
- `ios_screw_puzzle` 的前 5 个活跃版本为 1.15.0、1.14.0、1.13.0、1.10.0、1.6.0。其中主版本 `1.15.0` 的 latency 用户覆盖率为 86.69%。这一部分只能作为用户覆盖的粗略参考，不直接代表请求级流失。
- `screw_puzzle` 的前 5 个活跃版本为 1.16.0、1.14.0、1.15.0、1.12.0、1.8.0。其中主版本 `1.16.0` 的 latency 用户覆盖率为 72.25%。这一部分只能作为用户覆盖的粗略参考，不直接代表请求级流失。

### 数据

| product | app_version | dau_user_cnt | latency_user_cnt | latency_user_coverage_ratio | dau_user_share | latency_user_share |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | 299,542 | 259,668 | 86.69% | 91.44% | 94.49% |
| ios_screw_puzzle | 1.14.0 | 10,209 | 8,307 | 81.37% | 3.12% | 3.02% |
| ios_screw_puzzle | 1.13.0 | 4,315 | 3,474 | 80.51% | 1.32% | 1.26% |
| ios_screw_puzzle | 1.10.0 | 2,052 | 1,587 | 77.34% | 0.63% | 0.58% |
| ios_screw_puzzle | 1.6.0 | 1,553 | 0 | 0.00% | 0.47% | 0.00% |
| screw_puzzle | 1.16.0 | 651,545 | 470,721 | 72.25% | 86.14% | 93.22% |
| screw_puzzle | 1.14.0 | 33,886 | 14,882 | 43.92% | 4.48% | 2.95% |
| screw_puzzle | 1.15.0 | 29,202 | 14,534 | 49.77% | 3.86% | 2.88% |
| screw_puzzle | 1.12.0 | 8,372 | 3,253 | 38.86% | 1.11% | 0.64% |
| screw_puzzle | 1.8.0 | 6,257 | 0 | 0.00% | 0.83% | 0.00% |

### 指标说明
> - `dau_user_cnt`：该版本在 user_engagement 中的去重活跃用户数。
> - `latency_user_cnt`：该版本在 adslog_load_latency 中出现过的去重用户数。
> - `latency_user_coverage_ratio = latency_user_cnt / dau_user_cnt`。
> - 这部分只用于判断主版本是否大范围上报，不直接代表请求流失。

## 第二部分：请求匹配

### 结论
- `ios_screw_puzzle` 主版本的请求匹配率约为 86.84%，共有 3,893,630 个 request 没有匹配到 latency。相比用户覆盖，这一层更接近真实链路损耗。
- `screw_puzzle` 主版本的请求匹配率约为 70.81%，共有 19,196,715 个 request 没有匹配到 latency。相比用户覆盖，这一层更接近真实链路损耗。

### 数据

| product | target_version | ad_format | request_cnt | latency_request_cnt | matched_request_cnt | request_without_latency_cnt | latency_without_request_cnt | request_match_rate | latency_backfill_rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 0 | 2,476,270 | 0 | 0 | 2,476,270 | N/A | 0.00% |
| ios_screw_puzzle | 1.15.0 | interstitial | 19,401,944 | 17,874,354 | 17,693,769 | 1,708,175 | 180,585 | 91.20% | 98.99% |
| ios_screw_puzzle | 1.15.0 | rewarded | 10,182,406 | 8,077,527 | 7,996,951 | 2,185,455 | 80,576 | 78.54% | 99.00% |
| screw_puzzle | 1.16.0 | banner | 0 | 2,970,061 | 0 | 0 | 2,970,061 | N/A | 0.00% |
| screw_puzzle | 1.16.0 | interstitial | 42,155,937 | 32,557,763 | 32,217,631 | 9,938,306 | 340,132 | 76.42% | 98.96% |
| screw_puzzle | 1.16.0 | rewarded | 23,618,595 | 14,500,401 | 14,360,186 | 9,258,409 | 140,215 | 60.80% | 99.03% |

### 指标说明
> - `request_cnt`：主版本 adslog_request 中按 product + ad_format + user_pseudo_id + request_id 去重后的请求数。
> - `latency_request_cnt`：主版本 adslog_load_latency 中按相同键去重后的请求数。
> - `matched_request_cnt`：两边 request_id 成功匹配到的请求数。
> - `request_match_rate = matched_request_cnt / request_cnt`。

## 第三部分：AdMob 对比

### 3A. all_network_status

#### 表 1：已发起 AdMob 请求数 vs AdMob Bid requests

##### 结论
- `全部网络状态` 下，`已发起 AdMob 请求数` 相对 `AdMob Bid requests` 的恢复比例在 2.38% 到 29.93% 之间。
- 最接近 `AdMob Bid requests` 的点是 `ios_screw_puzzle/banner`，比例为 2.38%；偏差最大的点是 `screw_puzzle/rewarded`，比例为 29.93%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 63,352,600 | 1,504,696 | 61,847,904 | 2.38% |
| ios_screw_puzzle | 1.15.0 | interstitial | 10,528,976 | 253,578 | 10,275,398 | 2.41% |
| ios_screw_puzzle | 1.15.0 | rewarded | 6,164,077 | 239,783 | 5,924,294 | 3.89% |
| screw_puzzle | 1.16.0 | banner | 112,883,855 | 15,635,872 | 97,247,983 | 13.85% |
| screw_puzzle | 1.16.0 | interstitial | 14,259,318 | 2,397,283 | 11,862,035 | 16.81% |
| screw_puzzle | 1.16.0 | rewarded | 4,388,473 | 1,313,578 | 3,074,895 | 29.93% |

#### 表 2：全部 latency 扣 AdMob 未发起 vs AdMob Bid requests

##### 结论
- `全部网络状态` 下，`全部 latency 扣 AdMob 未发起` 相对 `AdMob Bid requests` 的恢复比例在 108.42% 到 370.87% 之间。
- 最接近 `AdMob Bid requests` 的点是 `screw_puzzle/banner`，比例为 108.42%；偏差最大的点是 `screw_puzzle/rewarded`，比例为 370.87%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 63,352,600 | 86,292,622 | -22,940,022 | 136.21% |
| ios_screw_puzzle | 1.15.0 | interstitial | 10,528,976 | 19,399,981 | -8,871,005 | 184.25% |
| ios_screw_puzzle | 1.15.0 | rewarded | 6,164,077 | 8,669,541 | -2,505,464 | 140.65% |
| screw_puzzle | 1.16.0 | banner | 112,883,855 | 122,387,674 | -9,503,819 | 108.42% |
| screw_puzzle | 1.16.0 | interstitial | 14,259,318 | 36,308,033 | -22,048,715 | 254.63% |
| screw_puzzle | 1.16.0 | rewarded | 4,388,473 | 16,275,531 | -11,887,058 | 370.87% |

#### 表 3：已发起 AdMob 请求数 vs AdMob Matched requests

##### 结论
- `全部网络状态` 下，`已发起 AdMob 请求数` 相对 `AdMob Matched requests` 的恢复比例在 42.86% 到 87.44% 之间。
- 最接近 `AdMob Matched requests` 的点是 `ios_screw_puzzle/rewarded`，比例为 42.86%；偏差最大的点是 `screw_puzzle/interstitial`，比例为 87.44%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 1,792,213 | 1,504,696 | 287,517 | 83.96% |
| ios_screw_puzzle | 1.15.0 | interstitial | 369,396 | 253,578 | 115,818 | 68.65% |
| ios_screw_puzzle | 1.15.0 | rewarded | 559,453 | 239,783 | 319,670 | 42.86% |
| screw_puzzle | 1.16.0 | banner | 18,141,863 | 15,635,872 | 2,505,991 | 86.19% |
| screw_puzzle | 1.16.0 | interstitial | 2,741,587 | 2,397,283 | 344,304 | 87.44% |
| screw_puzzle | 1.16.0 | rewarded | 1,518,747 | 1,313,578 | 205,169 | 86.49% |

#### 表 4：全部 latency 扣 AdMob 未发起 vs AdMob Matched requests

##### 结论
- `全部网络状态` 下，`全部 latency 扣 AdMob 未发起` 相对 `AdMob Matched requests` 的恢复比例在 674.61% 到 5251.81% 之间。
- 最接近 `AdMob Matched requests` 的点是 `screw_puzzle/banner`，比例为 674.61%；偏差最大的点是 `ios_screw_puzzle/interstitial`，比例为 5251.81%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 1,792,213 | 86,292,622 | -84,500,409 | 4814.86% |
| ios_screw_puzzle | 1.15.0 | interstitial | 369,396 | 19,399,981 | -19,030,585 | 5251.81% |
| ios_screw_puzzle | 1.15.0 | rewarded | 559,453 | 8,669,541 | -8,110,088 | 1549.65% |
| screw_puzzle | 1.16.0 | banner | 18,141,863 | 122,387,674 | -104,245,811 | 674.61% |
| screw_puzzle | 1.16.0 | interstitial | 2,741,587 | 36,308,033 | -33,566,446 | 1324.34% |
| screw_puzzle | 1.16.0 | rewarded | 1,518,747 | 16,275,531 | -14,756,784 | 1071.64% |

### 3B. online_only

#### 表 5：已发起 AdMob 请求数 vs AdMob Bid requests

##### 结论
- `仅有网` 下，`已发起 AdMob 请求数` 相对 `AdMob Bid requests` 的恢复比例在 2.37% 到 29.88% 之间。
- 最接近 `AdMob Bid requests` 的点是 `ios_screw_puzzle/banner`，比例为 2.37%；偏差最大的点是 `screw_puzzle/rewarded`，比例为 29.88%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 63,352,600 | 1,504,179 | 61,848,421 | 2.37% |
| ios_screw_puzzle | 1.15.0 | interstitial | 10,528,976 | 252,959 | 10,276,017 | 2.40% |
| ios_screw_puzzle | 1.15.0 | rewarded | 6,164,077 | 238,890 | 5,925,187 | 3.88% |
| screw_puzzle | 1.16.0 | banner | 112,883,855 | 15,619,957 | 97,263,898 | 13.84% |
| screw_puzzle | 1.16.0 | interstitial | 14,259,318 | 2,391,648 | 11,867,670 | 16.77% |
| screw_puzzle | 1.16.0 | rewarded | 4,388,473 | 1,311,426 | 3,077,047 | 29.88% |

#### 表 6：全部 latency 扣 AdMob 未发起 vs AdMob Bid requests

##### 结论
- `仅有网` 下，`全部 latency 扣 AdMob 未发起` 相对 `AdMob Bid requests` 的恢复比例在 108.30% 到 370.33% 之间。
- 最接近 `AdMob Bid requests` 的点是 `screw_puzzle/banner`，比例为 108.30%；偏差最大的点是 `screw_puzzle/rewarded`，比例为 370.33%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 63,352,600 | 86,257,613 | -22,905,013 | 136.15% |
| ios_screw_puzzle | 1.15.0 | interstitial | 10,528,976 | 19,376,563 | -8,847,587 | 184.03% |
| ios_screw_puzzle | 1.15.0 | rewarded | 6,164,077 | 8,658,308 | -2,494,231 | 140.46% |
| screw_puzzle | 1.16.0 | banner | 112,883,855 | 122,253,688 | -9,369,833 | 108.30% |
| screw_puzzle | 1.16.0 | interstitial | 14,259,318 | 36,244,356 | -21,985,038 | 254.18% |
| screw_puzzle | 1.16.0 | rewarded | 4,388,473 | 16,251,801 | -11,863,328 | 370.33% |

#### 表 7：已发起 AdMob 请求数 vs AdMob Matched requests

##### 结论
- `仅有网` 下，`已发起 AdMob 请求数` 相对 `AdMob Matched requests` 的恢复比例在 42.70% 到 87.24% 之间。
- 最接近 `AdMob Matched requests` 的点是 `ios_screw_puzzle/rewarded`，比例为 42.70%；偏差最大的点是 `screw_puzzle/interstitial`，比例为 87.24%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 1,792,213 | 1,504,179 | 288,034 | 83.93% |
| ios_screw_puzzle | 1.15.0 | interstitial | 369,396 | 252,959 | 116,437 | 68.48% |
| ios_screw_puzzle | 1.15.0 | rewarded | 559,453 | 238,890 | 320,563 | 42.70% |
| screw_puzzle | 1.16.0 | banner | 18,141,863 | 15,619,957 | 2,521,906 | 86.10% |
| screw_puzzle | 1.16.0 | interstitial | 2,741,587 | 2,391,648 | 349,939 | 87.24% |
| screw_puzzle | 1.16.0 | rewarded | 1,518,747 | 1,311,426 | 207,321 | 86.35% |

#### 表 8：全部 latency 扣 AdMob 未发起 vs AdMob Matched requests

##### 结论
- `仅有网` 下，`全部 latency 扣 AdMob 未发起` 相对 `AdMob Matched requests` 的恢复比例在 673.88% 到 5245.47% 之间。
- 最接近 `AdMob Matched requests` 的点是 `screw_puzzle/banner`，比例为 673.88%；偏差最大的点是 `ios_screw_puzzle/interstitial`，比例为 5245.47%。

##### 数据

| product | target_version | ad_format | backend_value | latency_value | gap | latency / backend |
| --- | --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 1,792,213 | 86,257,613 | -84,465,400 | 4812.91% |
| ios_screw_puzzle | 1.15.0 | interstitial | 369,396 | 19,376,563 | -19,007,167 | 5245.47% |
| ios_screw_puzzle | 1.15.0 | rewarded | 559,453 | 8,658,308 | -8,098,855 | 1547.64% |
| screw_puzzle | 1.16.0 | banner | 18,141,863 | 122,253,688 | -104,111,825 | 673.88% |
| screw_puzzle | 1.16.0 | interstitial | 2,741,587 | 36,244,356 | -33,502,769 | 1322.02% |
| screw_puzzle | 1.16.0 | rewarded | 1,518,747 | 16,251,801 | -14,733,054 | 1070.08% |

### 3C. network_status_breakdown

#### 表 9：admob_started_cnt 口径下的 online/offline/unknown 分布

##### 结论
- `ios_screw_puzzle/banner` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.97% / 0.03% / 0.00%。
- `ios_screw_puzzle/interstitial` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.76% / 0.24% / 0.00%。
- `ios_screw_puzzle/rewarded` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.63% / 0.37% / 0.00%。
- `screw_puzzle/banner` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.90% / 0.03% / 0.07%。
- `screw_puzzle/interstitial` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.76% / 0.13% / 0.10%。
- `screw_puzzle/rewarded` 在 `已发起 AdMob 请求数` 下，`online/offline/unknown` 占比分别为 99.84% / 0.09% / 0.08%。

##### 数据

| product | target_version | ad_format | status_name | pv_count | pv_ratio |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | online | 1,504,179 | 99.97% |
| ios_screw_puzzle | 1.15.0 | banner | offline | 517 | 0.03% |
| ios_screw_puzzle | 1.15.0 | interstitial | online | 252,959 | 99.76% |
| ios_screw_puzzle | 1.15.0 | interstitial | offline | 619 | 0.24% |
| ios_screw_puzzle | 1.15.0 | rewarded | online | 238,890 | 99.63% |
| ios_screw_puzzle | 1.15.0 | rewarded | offline | 893 | 0.37% |
| screw_puzzle | 1.16.0 | banner | online | 15,619,957 | 99.90% |
| screw_puzzle | 1.16.0 | banner | offline | 4,478 | 0.03% |
| screw_puzzle | 1.16.0 | banner | unknown | 11,437 | 0.07% |
| screw_puzzle | 1.16.0 | interstitial | online | 2,391,648 | 99.76% |
| screw_puzzle | 1.16.0 | interstitial | offline | 3,167 | 0.13% |
| screw_puzzle | 1.16.0 | interstitial | unknown | 2,468 | 0.10% |
| screw_puzzle | 1.16.0 | rewarded | online | 1,311,426 | 99.84% |
| screw_puzzle | 1.16.0 | rewarded | offline | 1,161 | 0.09% |
| screw_puzzle | 1.16.0 | rewarded | unknown | 991 | 0.08% |

#### 表 10：admob_total_minus_not_started_cnt 口径下的 online/offline/unknown 分布

##### 结论
- `ios_screw_puzzle/banner` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.96% / 0.04% / 0.00%。
- `ios_screw_puzzle/interstitial` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.91% / 0.09% / 0.00%。
- `ios_screw_puzzle/rewarded` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.90% / 0.10% / 0.00%。
- `screw_puzzle/banner` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.90% / 0.03% / 0.07%。
- `screw_puzzle/interstitial` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.86% / 0.05% / 0.09%。
- `screw_puzzle/rewarded` 在 `全部 latency 扣 AdMob 未发起` 下，`online/offline/unknown` 占比分别为 99.87% / 0.05% / 0.08%。

##### 数据

| product | target_version | ad_format | status_name | pv_count | pv_ratio |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | online | 89,340,166 | 99.96% |
| ios_screw_puzzle | 1.15.0 | banner | offline | 35,009 | 0.04% |
| ios_screw_puzzle | 1.15.0 | interstitial | online | 25,132,429 | 99.91% |
| ios_screw_puzzle | 1.15.0 | interstitial | offline | 23,418 | 0.09% |
| ios_screw_puzzle | 1.15.0 | rewarded | online | 11,333,976 | 99.90% |
| ios_screw_puzzle | 1.15.0 | rewarded | offline | 11,233 | 0.10% |
| screw_puzzle | 1.16.0 | banner | online | 134,563,533 | 99.90% |
| screw_puzzle | 1.16.0 | banner | offline | 36,448 | 0.03% |
| screw_puzzle | 1.16.0 | banner | unknown | 97,538 | 0.07% |
| screw_puzzle | 1.16.0 | interstitial | online | 44,705,033 | 99.86% |
| screw_puzzle | 1.16.0 | interstitial | offline | 21,840 | 0.05% |
| screw_puzzle | 1.16.0 | interstitial | unknown | 41,837 | 0.09% |
| screw_puzzle | 1.16.0 | rewarded | online | 18,506,489 | 99.87% |
| screw_puzzle | 1.16.0 | rewarded | offline | 8,796 | 0.05% |
| screw_puzzle | 1.16.0 | rewarded | unknown | 14,934 | 0.08% |

#### 表 11：online 内原始 lib_net_status 明细（admob_started_cnt 口径）

##### 结论
- `已发起 AdMob 请求数` 下 online 内占比最高的原始网络状态为：screw_puzzle/banner/WIFI = 10,830,605 (69.34%)；screw_puzzle/banner/4G = 4,193,445 (26.85%)；screw_puzzle/interstitial/WIFI = 1,685,568 (70.48%)；ios_screw_puzzle/banner/WIFI = 1,230,423 (81.80%)；screw_puzzle/rewarded/WIFI = 952,218 (72.61%)；screw_puzzle/interstitial/4G = 600,628 (25.11%)。

##### 数据

| product | target_version | ad_format | lib_net_status | pv_count | pv_ratio |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | WIFI | 1,230,423 | 81.80% |
| ios_screw_puzzle | 1.15.0 | banner | 4G | 149,587 | 9.94% |
| ios_screw_puzzle | 1.15.0 | banner | 5G | 117,431 | 7.81% |
| ios_screw_puzzle | 1.15.0 | banner | 3G | 5,589 | 0.37% |
| ios_screw_puzzle | 1.15.0 | banner | 2G | 835 | 0.06% |
| ios_screw_puzzle | 1.15.0 | banner | MOBILE | 281 | 0.02% |
| ios_screw_puzzle | 1.15.0 | banner | WIRED | 33 | 0.00% |
| ios_screw_puzzle | 1.15.0 | interstitial | WIFI | 203,222 | 80.34% |
| ios_screw_puzzle | 1.15.0 | interstitial | 4G | 27,277 | 10.78% |
| ios_screw_puzzle | 1.15.0 | interstitial | 5G | 20,996 | 8.30% |
| ios_screw_puzzle | 1.15.0 | interstitial | 3G | 1,248 | 0.49% |
| ios_screw_puzzle | 1.15.0 | interstitial | 2G | 148 | 0.06% |
| ios_screw_puzzle | 1.15.0 | interstitial | MOBILE | 61 | 0.02% |
| ios_screw_puzzle | 1.15.0 | interstitial | WIRED | 7 | 0.00% |
| ios_screw_puzzle | 1.15.0 | rewarded | WIFI | 185,388 | 77.60% |
| ios_screw_puzzle | 1.15.0 | rewarded | 4G | 30,127 | 12.61% |
| ios_screw_puzzle | 1.15.0 | rewarded | 5G | 21,992 | 9.21% |
| ios_screw_puzzle | 1.15.0 | rewarded | 3G | 1,226 | 0.51% |
| ios_screw_puzzle | 1.15.0 | rewarded | 2G | 109 | 0.05% |
| ios_screw_puzzle | 1.15.0 | rewarded | MOBILE | 39 | 0.02% |
| ios_screw_puzzle | 1.15.0 | rewarded | WIRED | 9 | 0.00% |
| screw_puzzle | 1.16.0 | banner | WIFI | 10,830,605 | 69.34% |
| screw_puzzle | 1.16.0 | banner | 4G | 4,193,445 | 26.85% |
| screw_puzzle | 1.16.0 | banner | 5G | 327,882 | 2.10% |
| screw_puzzle | 1.16.0 | banner | 3G | 195,224 | 1.25% |
| screw_puzzle | 1.16.0 | banner | 2G | 72,784 | 0.47% |
| screw_puzzle | 1.16.0 | banner | vpn | 17 | 0.00% |
| screw_puzzle | 1.16.0 | interstitial | WIFI | 1,685,568 | 70.48% |
| screw_puzzle | 1.16.0 | interstitial | 4G | 600,628 | 25.11% |
| screw_puzzle | 1.16.0 | interstitial | 5G | 51,154 | 2.14% |
| screw_puzzle | 1.16.0 | interstitial | 3G | 43,050 | 1.80% |
| screw_puzzle | 1.16.0 | interstitial | 2G | 11,248 | 0.47% |
| screw_puzzle | 1.16.0 | rewarded | WIFI | 952,218 | 72.61% |
| screw_puzzle | 1.16.0 | rewarded | 4G | 305,485 | 23.29% |
| screw_puzzle | 1.16.0 | rewarded | 5G | 28,569 | 2.18% |
| screw_puzzle | 1.16.0 | rewarded | 3G | 20,020 | 1.53% |
| screw_puzzle | 1.16.0 | rewarded | 2G | 5,134 | 0.39% |

#### 表 12：online 内原始 lib_net_status 明细（admob_total_minus_not_started_cnt 口径）

##### 结论
- `全部 latency 扣 AdMob 未发起` 下 online 内占比最高的原始网络状态为：screw_puzzle/banner/WIFI = 96,978,896 (72.07%)；ios_screw_puzzle/banner/WIFI = 67,051,769 (75.05%)；screw_puzzle/banner/4G = 32,451,160 (24.12%)；screw_puzzle/interstitial/WIFI = 31,468,864 (70.39%)；ios_screw_puzzle/interstitial/WIFI = 18,926,579 (75.31%)；screw_puzzle/rewarded/WIFI = 13,587,150 (73.42%)。

##### 数据

| product | target_version | ad_format | lib_net_status | pv_count | pv_ratio |
| --- | --- | --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | WIFI | 67,051,769 | 75.05% |
| ios_screw_puzzle | 1.15.0 | banner | 5G | 11,572,389 | 12.95% |
| ios_screw_puzzle | 1.15.0 | banner | 4G | 10,366,730 | 11.60% |
| ios_screw_puzzle | 1.15.0 | banner | 3G | 291,437 | 0.33% |
| ios_screw_puzzle | 1.15.0 | banner | 2G | 37,334 | 0.04% |
| ios_screw_puzzle | 1.15.0 | banner | MOBILE | 17,748 | 0.02% |
| ios_screw_puzzle | 1.15.0 | banner | WIRED | 2,759 | 0.00% |
| ios_screw_puzzle | 1.15.0 | interstitial | WIFI | 18,926,579 | 75.31% |
| ios_screw_puzzle | 1.15.0 | interstitial | 4G | 3,244,626 | 12.91% |
| ios_screw_puzzle | 1.15.0 | interstitial | 5G | 2,817,861 | 11.21% |
| ios_screw_puzzle | 1.15.0 | interstitial | 3G | 124,483 | 0.50% |
| ios_screw_puzzle | 1.15.0 | interstitial | 2G | 13,057 | 0.05% |
| ios_screw_puzzle | 1.15.0 | interstitial | MOBILE | 5,202 | 0.02% |
| ios_screw_puzzle | 1.15.0 | interstitial | WIRED | 621 | 0.00% |
| ios_screw_puzzle | 1.15.0 | rewarded | WIFI | 8,642,993 | 76.26% |
| ios_screw_puzzle | 1.15.0 | rewarded | 4G | 1,381,177 | 12.19% |
| ios_screw_puzzle | 1.15.0 | rewarded | 5G | 1,253,734 | 11.06% |
| ios_screw_puzzle | 1.15.0 | rewarded | 3G | 49,559 | 0.44% |
| ios_screw_puzzle | 1.15.0 | rewarded | 2G | 4,838 | 0.04% |
| ios_screw_puzzle | 1.15.0 | rewarded | MOBILE | 1,446 | 0.01% |
| ios_screw_puzzle | 1.15.0 | rewarded | WIRED | 229 | 0.00% |
| screw_puzzle | 1.16.0 | banner | WIFI | 96,978,896 | 72.07% |
| screw_puzzle | 1.16.0 | banner | 4G | 32,451,160 | 24.12% |
| screw_puzzle | 1.16.0 | banner | 5G | 3,135,852 | 2.33% |
| screw_puzzle | 1.16.0 | banner | 3G | 1,473,977 | 1.10% |
| screw_puzzle | 1.16.0 | banner | 2G | 523,491 | 0.39% |
| screw_puzzle | 1.16.0 | banner | vpn | 157 | 0.00% |
| screw_puzzle | 1.16.0 | interstitial | WIFI | 31,468,864 | 70.39% |
| screw_puzzle | 1.16.0 | interstitial | 4G | 11,308,795 | 25.30% |
| screw_puzzle | 1.16.0 | interstitial | 5G | 1,089,254 | 2.44% |
| screw_puzzle | 1.16.0 | interstitial | 3G | 636,945 | 1.42% |
| screw_puzzle | 1.16.0 | interstitial | 2G | 201,118 | 0.45% |
| screw_puzzle | 1.16.0 | interstitial | vpn | 57 | 0.00% |
| screw_puzzle | 1.16.0 | rewarded | WIFI | 13,587,150 | 73.42% |
| screw_puzzle | 1.16.0 | rewarded | 4G | 4,198,417 | 22.69% |
| screw_puzzle | 1.16.0 | rewarded | 5G | 407,970 | 2.20% |
| screw_puzzle | 1.16.0 | rewarded | 3G | 240,177 | 1.30% |
| screw_puzzle | 1.16.0 | rewarded | 2G | 72,725 | 0.39% |
| screw_puzzle | 1.16.0 | rewarded | vpn | 50 | 0.00% |

### 指标说明
> - `admob_started_cnt`：指定 6 个 AdMob placement 中，status 为 `-2/-3` 的数量。
> - `admob_total_minus_not_started_cnt`：全部 latency 事件数减去 AdMob 未发起数。
> - `all_network_status`：同时包含 online / offline / unknown。
> - `online_only`：只保留全部 latency 事件中的 `network_status_group = online` 部分。
> - `offline`：`lib_net_status = network-null`。
> - `unknown`：`lib_net_status = network-unknown` 或空串。
> - `online`：除 `network-null`、`network-unknown`、空串外的其他网络状态。

## 第四部分：MAX 后台数据

### 结论
- 这一部分只保留 MAX 后台的原始 attempts，作为第三个数据源单独展示，不和 AdMob 对比表混写。

### 数据

| product | target_version | ad_format | max_attempts |
| --- | --- | --- | --- |
| ios_screw_puzzle | 1.15.0 | banner | 61,646,892 |
| ios_screw_puzzle | 1.15.0 | interstitial | 10,405,705 |
| ios_screw_puzzle | 1.15.0 | rewarded | 6,275,023 |
| screw_puzzle | 1.16.0 | banner | 116,621,765 |
| screw_puzzle | 1.16.0 | interstitial | 38,194,317 |
| screw_puzzle | 1.16.0 | rewarded | 17,423,043 |

### 指标说明
> - `max_attempts`：来自用户提供截图的 MAX 后台 attempts 数据。

## SQL 附录

### 1. 主版本覆盖 SQL
```sql
-- 查询目的：按产品统计主版本覆盖，只展示每个产品 DAU 前 5 的版本。
WITH version_events AS (
  SELECT
    'screw_puzzle' AS product,
    COALESCE(app_info.version, 'unknown') AS app_version,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('user_engagement', 'adslog_load_latency')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    COALESCE(app_info.version, 'unknown') AS app_version,
    user_pseudo_id,
    event_name
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND event_name IN ('user_engagement', 'adslog_load_latency')
),
active_users AS (
  SELECT
    product,
    app_version,
    COUNT(DISTINCT user_pseudo_id) AS dau_user_cnt
  FROM version_events
  WHERE event_name = 'user_engagement'
  GROUP BY product, app_version
),
latency_users AS (
  SELECT
    product,
    app_version,
    COUNT(DISTINCT user_pseudo_id) AS latency_user_cnt
  FROM version_events
  WHERE event_name = 'adslog_load_latency'
  GROUP BY product, app_version
),
version_totals AS (
  SELECT
    product,
    SUM(dau_user_cnt) AS total_dau_user_cnt,
    SUM(latency_user_cnt) AS total_latency_user_cnt
  FROM (
    SELECT product, app_version, dau_user_cnt, 0 AS latency_user_cnt
    FROM active_users
    UNION ALL
    SELECT product, app_version, 0 AS dau_user_cnt, latency_user_cnt
    FROM latency_users
  )
  GROUP BY product
),
ranked_versions AS (
  SELECT
    a.product,
    a.app_version,
    a.dau_user_cnt,
    COALESCE(l.latency_user_cnt, 0) AS latency_user_cnt,
    SAFE_DIVIDE(COALESCE(l.latency_user_cnt, 0), a.dau_user_cnt) AS latency_user_coverage_ratio,
    SAFE_DIVIDE(a.dau_user_cnt, t.total_dau_user_cnt) AS dau_user_share,
    SAFE_DIVIDE(COALESCE(l.latency_user_cnt, 0), NULLIF(t.total_latency_user_cnt, 0)) AS latency_user_share,
    ROW_NUMBER() OVER (
      PARTITION BY a.product
      ORDER BY a.dau_user_cnt DESC, a.app_version
    ) AS version_rank
  FROM active_users a
  LEFT JOIN latency_users l
    ON a.product = l.product
   AND a.app_version = l.app_version
  LEFT JOIN version_totals t
    ON a.product = t.product
)
SELECT
  product,
  app_version,
  dau_user_cnt,
  latency_user_cnt,
  latency_user_coverage_ratio,
  dau_user_share,
  latency_user_share
FROM ranked_versions
WHERE version_rank <= 5
ORDER BY product, dau_user_cnt DESC, app_version
```

### 2. 请求匹配 SQL
```sql
-- 查询目的：只看安卓 1.16.0 和 iOS 1.15.0，按 request_id 统计 adslog_request 与 adslog_load_latency 的匹配情况。
WITH all_events AS (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    user_pseudo_id,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.16.0'
    AND event_name IN ('adslog_request', 'adslog_load_latency')

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    '1.15.0' AS target_version,
    user_pseudo_id,
    event_name,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')),
      CAST((SELECT value.int_value FROM UNNEST(event_params.array) WHERE key IN ('request_key', 'request_id')) AS STRING)
    ) AS request_id
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name IN ('adslog_request', 'adslog_load_latency')
),
request_keys AS (
  SELECT
    product,
    target_version,
    ad_format,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_request'
    AND request_id IS NOT NULL
  GROUP BY product, target_version, ad_format, user_pseudo_id, request_id
),
latency_keys AS (
  SELECT
    product,
    target_version,
    ad_format,
    user_pseudo_id,
    request_id
  FROM all_events
  WHERE event_name = 'adslog_load_latency'
    AND request_id IS NOT NULL
  GROUP BY product, target_version, ad_format, user_pseudo_id, request_id
)
SELECT
  COALESCE(r.product, l.product) AS product,
  COALESCE(r.target_version, l.target_version) AS target_version,
  COALESCE(r.ad_format, l.ad_format) AS ad_format,
  COUNTIF(r.request_id IS NOT NULL) AS request_cnt,
  COUNTIF(l.request_id IS NOT NULL) AS latency_request_cnt,
  COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL) AS matched_request_cnt,
  COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NULL) AS request_without_latency_cnt,
  COUNTIF(r.request_id IS NULL AND l.request_id IS NOT NULL) AS latency_without_request_cnt,
  SAFE_DIVIDE(
    COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL),
    NULLIF(COUNTIF(r.request_id IS NOT NULL), 0)
  ) AS request_match_rate,
  SAFE_DIVIDE(
    COUNTIF(r.request_id IS NOT NULL AND l.request_id IS NOT NULL),
    NULLIF(COUNTIF(l.request_id IS NOT NULL), 0)
  ) AS latency_backfill_rate
FROM request_keys r
FULL OUTER JOIN latency_keys l
  ON r.product = l.product
 AND r.target_version = l.target_version
 AND r.ad_format = l.ad_format
 AND r.user_pseudo_id = l.user_pseudo_id
 AND r.request_id = l.request_id
GROUP BY
  COALESCE(r.product, l.product),
  COALESCE(r.target_version, l.target_version),
  COALESCE(r.ad_format, l.ad_format)
ORDER BY product, ad_format
```

### 3. AdMob 对比 SQL
```sql
-- 查询目的：只看安卓 1.16.0 和 iOS 1.15.0，统计指定 AdMob placement 的 started 口径，
-- 同时把 total_minus_not_started 改成“全部 latency 事件数减去 AdMob 未发起数”。
WITH all_latency_base AS (
  SELECT
    'screw_puzzle' AS product,
    '1.16.0' AS target_version,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.16.0'
    AND event_name = 'adslog_load_latency'

  UNION ALL

  SELECT
    'ios_screw_puzzle' AS product,
    '1.15.0' AS target_version,
    CASE
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 0 THEN 'banner'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 1 THEN 'interstitial'
      WHEN (SELECT value.int_value FROM UNNEST(event_params.array) WHERE key = 'ad_format') = 2 THEN 'rewarded'
      ELSE 'unknown'
    END AS ad_format,
    COALESCE(
      (SELECT value.string_value FROM UNNEST(event_params.array) WHERE key = 'lib_net_status'),
      ''
    ) AS lib_net_status,
    event_params.array AS event_params
  FROM `transferred.hudi_ods.ios_screw_puzzle`
  WHERE event_date BETWEEN '2026-01-05' AND '2026-01-12'
    AND app_info.version = '1.15.0'
    AND event_name = 'adslog_load_latency'
),
all_latency_status_rows AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    CASE
      WHEN lib_net_status = 'network-null' THEN 'offline'
      WHEN lib_net_status = 'network-unknown' OR lib_net_status = '' THEN 'unknown'
      ELSE 'online'
    END AS network_status_group
  FROM all_latency_base
),
admob_rows AS (
  SELECT
    b.product,
    b.target_version,
    b.ad_format,
    b.lib_net_status,
    CASE
      WHEN b.lib_net_status = 'network-null' THEN 'offline'
      WHEN b.lib_net_status = 'network-unknown' OR b.lib_net_status = '' THEN 'unknown'
      ELSE 'online'
    END AS network_status_group,
    SPLIT(
      COALESCE(
        ep.value.string_value,
        CAST(ep.value.int_value AS STRING),
        CAST(ep.value.double_value AS STRING)
      ),
      '|'
    )[SAFE_OFFSET(0)] AS placement_id,
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
  WHERE STARTS_WITH(ep.key, 'AdMob_')
),
admob_filtered_rows AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    network_status_group,
    fill_status_code
  FROM admob_rows
  WHERE placement_id IN (
    'ca-app-pub-9205389740674078/1370451014',
    'ca-app-pub-9205389740674078/8776645237',
    'ca-app-pub-9205389740674078/6562140933',
    'ca-app-pub-9205389740674078/6135800336',
    'ca-app-pub-9205389740674078/3484912845',
    'ca-app-pub-9205389740674078/2985858001'
  )
),
admob_not_started_counts AS (
  SELECT
    product,
    target_version,
    ad_format,
    COUNTIF(fill_status_code = '-1') AS admob_not_started_cnt
  FROM admob_filtered_rows
  GROUP BY product, target_version, ad_format
),
admob_started_scope_all AS (
  SELECT
    product,
    target_version,
    ad_format,
    'all_network_status' AS scope_name,
    COUNT(*) AS admob_started_cnt
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
  GROUP BY product, target_version, ad_format
),
admob_started_scope_online AS (
  SELECT
    product,
    target_version,
    ad_format,
    'online_only' AS scope_name,
    COUNT(*) AS admob_started_cnt
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
    AND network_status_group = 'online'
  GROUP BY product, target_version, ad_format
),
all_latency_scope_all AS (
  SELECT
    product,
    target_version,
    ad_format,
    'all_network_status' AS scope_name,
    COUNT(*) AS all_latency_cnt
  FROM all_latency_status_rows
  GROUP BY product, target_version, ad_format
),
all_latency_scope_online AS (
  SELECT
    product,
    target_version,
    ad_format,
    'online_only' AS scope_name,
    COUNT(*) AS all_latency_cnt
  FROM all_latency_status_rows
  WHERE network_status_group = 'online'
  GROUP BY product, target_version, ad_format
),
comparison_base AS (
  SELECT
    a.product,
    a.target_version,
    a.ad_format,
    a.scope_name,
    a.admob_started_cnt,
    l.all_latency_cnt - COALESCE(n.admob_not_started_cnt, 0) AS admob_total_minus_not_started_cnt
  FROM (
    SELECT * FROM admob_started_scope_all
    UNION ALL
    SELECT * FROM admob_started_scope_online
  ) a
  JOIN (
    SELECT * FROM all_latency_scope_all
    UNION ALL
    SELECT * FROM all_latency_scope_online
  ) l
    ON a.product = l.product
   AND a.target_version = l.target_version
   AND a.ad_format = l.ad_format
   AND a.scope_name = l.scope_name
  LEFT JOIN admob_not_started_counts n
    ON a.product = n.product
   AND a.target_version = n.target_version
   AND a.ad_format = n.ad_format
),
started_network_breakdown AS (
  SELECT
    product,
    target_version,
    ad_format,
    network_status_group,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
  GROUP BY product, target_version, ad_format, network_status_group
),
total_minus_not_started_network_breakdown AS (
  SELECT
    product,
    target_version,
    ad_format,
    network_status_group,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM all_latency_status_rows
  GROUP BY product, target_version, ad_format, network_status_group
),
started_online_status_detail AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM admob_filtered_rows
  WHERE fill_status_code IN ('-2', '-3')
    AND network_status_group = 'online'
  GROUP BY product, target_version, ad_format, lib_net_status
),
total_minus_not_started_online_status_detail AS (
  SELECT
    product,
    target_version,
    ad_format,
    lib_net_status,
    COUNT(*) AS pv_count,
    SAFE_DIVIDE(
      COUNT(*),
      NULLIF(SUM(COUNT(*)) OVER (PARTITION BY product, target_version, ad_format), 0)
    ) AS pv_ratio
  FROM all_latency_status_rows
  WHERE network_status_group = 'online'
  GROUP BY product, target_version, ad_format, lib_net_status
)
SELECT
  'comparison_base' AS report_section,
  product,
  target_version,
  ad_format,
  scope_name,
  NULL AS basis_name,
  NULL AS status_name,
  NULL AS pv_count,
  NULL AS pv_ratio,
  admob_started_cnt,
  admob_total_minus_not_started_cnt
FROM comparison_base

UNION ALL

SELECT
  'network_breakdown' AS report_section,
  product,
  target_version,
  ad_format,
  'all_network_status' AS scope_name,
  'admob_started_cnt' AS basis_name,
  network_status_group AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM started_network_breakdown

UNION ALL

SELECT
  'network_breakdown' AS report_section,
  product,
  target_version,
  ad_format,
  'all_network_status' AS scope_name,
  'admob_total_minus_not_started_cnt' AS basis_name,
  network_status_group AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM total_minus_not_started_network_breakdown

UNION ALL

SELECT
  'online_status_detail' AS report_section,
  product,
  target_version,
  ad_format,
  'online_only' AS scope_name,
  'admob_started_cnt' AS basis_name,
  lib_net_status AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM started_online_status_detail

UNION ALL

SELECT
  'online_status_detail' AS report_section,
  product,
  target_version,
  ad_format,
  'online_only' AS scope_name,
  'admob_total_minus_not_started_cnt' AS basis_name,
  lib_net_status AS status_name,
  pv_count,
  pv_ratio,
  NULL AS admob_started_cnt,
  NULL AS admob_total_minus_not_started_cnt
FROM total_minus_not_started_online_status_detail

ORDER BY report_section, product, ad_format, scope_name, basis_name, status_name
```
