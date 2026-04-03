SELECT column_name, data_type
FROM `transferred.dwd.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'dwd_tradplus_rt'
ORDER BY ordinal_position