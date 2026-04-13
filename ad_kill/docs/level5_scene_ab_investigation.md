# 第5关 Scene 分布 AB 差异调查

## 背景

Dashboard 2 的 scene 分布显示 AB 两组在第5关存在差异（B 组 long_watch_kill 占比更高，none 占比更低），但理论上第5关是 AB 策略生效的起点，不应该有行为差异。

## 已确认的事实

### 业务逻辑
- 第5关是第一个有广告的关卡，AB 分组策略基于第5关的广告行为决定后续路径
- 关卡线性向前，不能退回重玩
- scene 参数有一个事件的延迟：game_new_start 上的 scene 反映的是**本关截至上一局**的杀广告累计情况，当局杀广告的结果需要下一次事件才能体现
- B 组跳关机制：用户在第5关杀广告符合条件后，客户端会自动补发一次 game_new_start（levelid=5）+ game_win，用于前端显示 next 页面让用户跳转下一关。用户实际看不到这次开局和胜利

### 数据验证结果

1. **杀广告次数分布 AB 一致**（`ad_kill/data/level5_kill_count_distribution.csv`）
   - ball_sort: A 87.20% vs B 87.19%（0次），A 9.93% vs B 9.93%（1次）
   - ios_nuts_sort: A 74.38% vs B 74.36%（0次），A 21.31% vs B 21.35%（1次）
   - 结论：用户真实的杀广告行为在 AB 之间无差异

2. **开局次数分布 AB 有明显差异**（`ad_kill/data/level5_start_count_and_ttk.csv`）
   - B 组 start_count=2 占比明显偏高（BS: A 26.01% vs B 28.09%，NS: A 21.58% vs B 26.78%）
   - B 组 start_count≥4 占比明显偏低
   - 原因：B 组跳关用户多了一次"幽灵" game_new_start 事件

3. **scene=none 用户中有杀广告行为**
   - ball_sort: A 6.43% / B 5.39% 的 none 用户实际有 lib_fullscreen_ad_killed 事件
   - ios_nuts_sort: A 13.86% / B 12.27%
   - 原因：最后一局杀广告后没有下一次事件来更新 scene

4. **time_to_kill 全部为 null** — lib_fullscreen_ad_killed 事件上没有 time_to_kill 参数

## 当前结论

Scene 分布的 AB 差异由两个因素叠加导致：
1. **scene 统计延迟**：当局杀广告结果需要下一次事件才能体现
2. **B 组幽灵事件**：跳关机制多发一次 game_new_start（levelid=5），这次事件能捕获到最后一次杀广告的 scene，而 A 组没有这个额外事件

本质上不是用户行为差异，而是统计口径差异。

## 待解决问题

**修正 B 组开局次数统计**：对于 B 组用户，需要去掉跳关产生的幽灵 game_new_start，才能得到真实的开局次数。

需要确定如何识别幽灵事件：
- 方案 A：看 B 组用户第5关最后一条 game_new_start 紧接 game_win，且时间间隔极短
- 方案 B：是否有跳关专门的埋点事件
- 方案 C：其他识别方式

确认识别方式后，可以：
1. 修正开局次数统计
2. 用修正后的数据重新计算 scene 分布，验证 AB 是否一致
3. 更新 Dashboard 2 的结论

## 相关文件

| 文件 | 说明 |
|------|------|
| `ad_kill/sql/verify_none_scene_kill_count.sql` | 验证 scene=none 用户的杀广告行为 |
| `ad_kill/sql/verify_level5_kill_distribution.sql` | 第5关杀广告次数分布 |
| `ad_kill/sql/verify_level5_start_and_ttk.sql` | 第5关开局次数 + time_to_kill 分布 |
| `ad_kill/data/level5_kill_count_distribution.csv` | 杀广告次数分布数据 |
| `ad_kill/data/level5_start_count_and_ttk.csv` | 开局次数 + ttk 数据 |
| `ad_kill/scripts/gen_scene_dashboard.py` | Dashboard 生成脚本（已加入 kill count 图表） |
