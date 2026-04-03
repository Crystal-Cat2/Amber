# car_jam Android TradPlus ADX AB rerun

## 执行信息
- execution_path = `local_bigquery`
- SQL = [car_jam_tp_adx_ab_tradplus_rt.sql](D:/Work/Amber/daily_tasks/sql/car_jam_tp_adx_ab_tradplus_rt.sql)
- output_csv = [2026-04-03_car_jam_tp_adx_ab_tradplus_rt.csv](D:/Work/Amber/daily_tasks/data/2026-04-03_car_jam_tp_adx_ab_tradplus_rt.csv)
- job_id = `3b896297-cadb-4b70-8993-c314f87610f1`
- elapsed_sec = `362`
- row_count = `8750`
- dry_run_bytes = `11225028867`

## 口径说明
- 产品仅包含 `car_jam` Android。
- AB 分组取 `lib_tpx_group` 事件中的 `group`，`A = no_tp_adx`，`B = have_tp_adx`。
- MAX 数据按 `user_id + min/max event_timestamp window` 归因。
- TradPlus 数据源使用 `transferred.dwd.dwd_tradplus_rt`，按 `user_id` 归入 AB window。
- GAP 口径为 `have_tp_adx - no_tp_adx`，即 `B - A`。

## 总览
| ad_format | impression_gap | revenue_gap | ecpm_gap | impression -> no_tp_adx | impression -> have_tp_adx | revenue -> no_tp_adx | revenue -> have_tp_adx | ecpm -> no_tp_adx | ecpm -> have_tp_adx |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BANNER | -81942 | 1128.59 | 0.03 | 39462565 | 39380623 | 7501.99 | 8630.58 | 0.19 | 0.22 |
| INTER | -108370 | 946.62 | 0.48 | 5036491 | 4928121 | 65887.42 | 66834.04 | 13.08 | 13.56 |
| REWARD | -19927 | -784.39 | -0.37 | 630314 | 610387 | 17759.56 | 16975.17 | 28.18 | 27.81 |

## 当前阻塞
- 飞书文档 URL: `https://xwbo3y4nxr.feishu.cn/docx/Tx6rdv0kGoTW16x6F8WcS6lOnrf`
- `lark-cli docs +fetch` / `docs +update` 当前都返回：
  `failed to get access token: need_user_authorization`
- 尝试 `lark-cli auth login --domain docs` 后，CLI 进一步报错：
  `device authorization failed: The request is missing a required parameter: client_secret.`
- 因此本次已完成重跑和本地落盘，但飞书文档尚未自动更新。
