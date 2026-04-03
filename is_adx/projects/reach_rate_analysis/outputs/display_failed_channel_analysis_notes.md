# display_failed 渠道分析说明

- 时间范围：event_date 2025-09-18 到 2026-01-03
- 产品：screw_puzzle、ios_screw_puzzle
- 广告格式：interstitial、rewarded
- AB 分组：基于 lib_isx_group 的 user_id + experiment_group 全周期 min/max 窗口
- 表 1：同一渠道行同时输出 trigger / show / impression / display_failed 的 pv，以及 show_rate / impression_rate
- 表 2：display_failed 的分渠道失败原因分布

## ios_screw_puzzle | interstitial | have_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 789953 | 755830 | 95.68% | 703970 | 93.14% | 16005 | 38.98% |
| IsAdxCustomAdapter | 1006692 | 969404 | 96.30% | 943597 | 97.34% | 5151 | 12.55% |
| Mintegral | 248533 | 238567 | 95.99% | 228225 | 95.66% | 4910 | 11.96% |
| Ogury | 232352 | 226849 | 97.63% | 220879 | 97.37% | 4595 | 11.19% |
| InMobi | 251342 | 242285 | 96.40% | 237870 | 98.18% | 3307 | 8.05% |

- 头部失败渠道：`Unity Ads`，trigger pv `789953`，show pv `755830`，show_rate `95.68%`，impression pv `703970`，impression_rate `93.14%`，display_failed pv `16005`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_Video failed to start`: 7036 (43.96%)
  - `-4205_Ad Display Failed/6_`: 5963 (37.26%)
  - `-4205_Ad Display Failed/7_Failed invoke WebView the method:  show`: 1256 (7.85%)

## ios_screw_puzzle | interstitial | no_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 1137789 | 1086128 | 95.46% | 1008521 | 92.85% | 21809 | 53.33% |
| Mintegral | 283104 | 273926 | 96.76% | 261714 | 95.54% | 5879 | 14.38% |
| InMobi | 306785 | 297930 | 97.11% | 292479 | 98.17% | 4299 | 10.51% |
| HyprMX | 18896 | 18556 | 98.20% | 16889 | 91.02% | 1656 | 4.05% |
| ironSource | 144898 | 139307 | 96.14% | 137452 | 98.67% | 1484 | 3.63% |

- 头部失败渠道：`Unity Ads`，trigger pv `1137789`，show pv `1086128`，show_rate `95.46%`，impression pv `1008521`，impression_rate `92.85%`，display_failed pv `21809`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 10727 (49.19%)
  - `-4205_Ad Display Failed/6_Video failed to start`: 7238 (33.19%)
  - `-4205_Ad Display Failed/7_Failed invoke WebView the method:  show`: 1319 (6.05%)

## ios_screw_puzzle | rewarded | have_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 180815 | 180799 | 99.99% | 173133 | 95.76% | 7192 | 42.45% |
| IsAdxCustomAdapter | 222999 | 222995 | 100.00% | 218924 | 98.17% | 3625 | 21.40% |
| Liftoff_custom | 133751 | 133744 | 99.99% | 130529 | 97.60% | 2420 | 14.28% |
| Google AdMob | 626536 | 626525 | 100.00% | 625090 | 99.77% | 1392 | 8.22% |
| Liftoff Monetize | 506653 | 506633 | 100.00% | 503499 | 99.38% | 538 | 3.18% |

- 头部失败渠道：`Unity Ads`，trigger pv `180815`，show pv `180799`，show_rate `99.99%`，impression pv `173133`，impression_rate `95.76%`，display_failed pv `7192`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_Video failed to start`: 2118 (29.45%)
  - `-4205_Ad Display Failed/7_Failed invoke WebView the method:  show`: 1873 (26.04%)
  - `-4205_Ad Display Failed/6_`: 1720 (23.92%)

## ios_screw_puzzle | rewarded | no_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 243511 | 243495 | 99.99% | 235125 | 96.56% | 7732 | 53.13% |
| Liftoff_custom | 158785 | 158781 | 100.00% | 155060 | 97.66% | 3027 | 20.80% |
| ironSource | 69737 | 69737 | 100.00% | 68508 | 98.24% | 1091 | 7.50% |
| Google AdMob | 684640 | 684636 | 100.00% | 683764 | 99.87% | 858 | 5.90% |
| Liftoff Monetize | 538456 | 538441 | 100.00% | 535882 | 99.52% | 513 | 3.52% |

- 头部失败渠道：`Unity Ads`，trigger pv `243511`，show pv `243495`，show_rate `99.99%`，impression pv `235125`，impression_rate `96.56%`，display_failed pv `7732`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 2666 (34.48%)
  - `-4205_Ad Display Failed/6_Video failed to start`: 1872 (24.21%)
  - `-4205_Ad Display Failed/7_Failed invoke WebView the method:  show`: 1520 (19.66%)

## screw_puzzle | interstitial | have_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 2647420 | 2644459 | 99.89% | 1993137 | 75.37% | 175841 | 28.66% |
| Mintegral | 1176301 | 1175325 | 99.92% | 926682 | 78.84% | 93595 | 15.25% |
| MaticooCustomAdapter | 260555 | 260456 | 99.96% | 170183 | 65.34% | 68483 | 11.16% |
| BidMachine | 339052 | 338777 | 99.92% | 241609 | 71.32% | 53748 | 8.76% |
| AppLovin | 7806996 | 7800491 | 99.92% | 6693174 | 85.80% | 38594 | 6.29% |

- 头部失败渠道：`Unity Ads`，trigger pv `2647420`，show pv `2644459`，show_rate `99.89%`，impression pv `1993137`，impression_rate `75.37%`，display_failed pv `175841`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 94519 (53.75%)
  - `-5602_Ad failed to display! Please disable the "Don't Keep Activities" setting in your developer set`: 32756 (18.63%)
  - `-4205_Ad Display Failed/0_[UnityAds] SDK not initialized`: 15758 (8.96%)

## screw_puzzle | interstitial | no_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 2860764 | 2857424 | 99.88% | 2198746 | 76.95% | 156657 | 26.39% |
| Mintegral | 1334493 | 1333521 | 99.93% | 1058500 | 79.38% | 96787 | 16.30% |
| MaticooCustomAdapter | 291336 | 291195 | 99.95% | 189237 | 64.99% | 80842 | 13.62% |
| BidMachine | 427902 | 427578 | 99.92% | 298523 | 69.82% | 77218 | 13.01% |
| Google AdMob | 8444277 | 8438435 | 99.93% | 8041574 | 95.30% | 28829 | 4.86% |

- 头部失败渠道：`Unity Ads`，trigger pv `2860764`，show pv `2857424`，show_rate `99.88%`，impression pv `2198746`，impression_rate `76.95%`，display_failed pv `156657`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 85091 (54.32%)
  - `-5602_Ad failed to display! Please disable the "Don't Keep Activities" setting in your developer set`: 28830 (18.40%)
  - `-4205_Ad Display Failed/0_[UnityAds] SDK not initialized`: 13395 (8.55%)

## screw_puzzle | rewarded | have_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 612950 | 612940 | 100.00% | 558449 | 91.11% | 21900 | 20.45% |
| Facebook | 1481996 | 1481942 | 100.00% | 1366820 | 92.23% | 17935 | 16.74% |
| IsAdxCustomAdapter | 465766 | 465760 | 100.00% | 417704 | 89.68% | 15879 | 14.82% |
| MaticooCustomAdapter | 76652 | 76650 | 100.00% | 60304 | 78.67% | 12919 | 12.06% |
| AppLovin | 3979728 | 3979706 | 100.00% | 3618157 | 90.92% | 8331 | 7.78% |

- 头部失败渠道：`Unity Ads`，trigger pv `612950`，show pv `612940`，show_rate `100.00%`，impression pv `558449`，impression_rate `91.11%`，display_failed pv `21900`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 9952 (45.44%)
  - `-4205_Ad Display Failed/0_[UnityAds] SDK not initialized`: 4916 (22.45%)
  - `-23_Attempting to show ad when another fullscreen ad is already showing/-1_`: 1565 (7.15%)

## screw_puzzle | rewarded | no_is_adx 组
| network_name | trigger_pv | show_pv | show_rate | impression_pv | impression_rate | display_failed_pv | display_failed_share |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unity Ads | 670123 | 670121 | 100.00% | 615014 | 91.78% | 20618 | 22.57% |
| Facebook | 1527195 | 1527159 | 100.00% | 1412202 | 92.47% | 16736 | 18.32% |
| MaticooCustomAdapter | 79182 | 79181 | 100.00% | 62114 | 78.45% | 13673 | 14.97% |
| AppLovin | 4021786 | 4021777 | 100.00% | 3681161 | 91.53% | 7561 | 8.28% |
| InMobi | 570004 | 569990 | 100.00% | 551944 | 96.83% | 5075 | 5.56% |

- 头部失败渠道：`Unity Ads`，trigger pv `670123`，show pv `670121`，show_rate `100.00%`，impression pv `615014`，impression_rate `91.78%`，display_failed pv `20618`。
- 该渠道头部失败原因：
  - `-4205_Ad Display Failed/6_`: 9813 (47.59%)
  - `-4205_Ad Display Failed/0_[UnityAds] SDK not initialized`: 3612 (17.52%)
  - `-4205_Ad Display Failed/5_Can't show a new ad unit when ad unit is already open`: 1762 (8.55%)
