-- 测试用 SQL，验证 git diff 流程
SELECT 1 AS test_col
FROM `transferred.hudi_ods.ball_sort`
WHERE event_date = '2026-04-01'
