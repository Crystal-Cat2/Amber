SELECT User_ID
FROM `gpdata-224001.applovin_max.car_jam_*`
WHERE _TABLE_SUFFIX = '20260401'
  AND User_ID IS NOT NULL
  AND LOWER(Ad_placement) LIKE 'tradplusadx%'
LIMIT 10