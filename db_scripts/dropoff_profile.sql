DROP TABLE IF EXISTS calc.dropoffs_profile;
CREATE TABLE calc.dropoffs_profile as
WITH fullgrid as
(select A.dropoff_grid
		,B.dropoff_weekday
		,C.dropoff_hour
FROM (SELECT dropoff_grid from calc.ride_request_data_calc group by dropoff_grid) as A
CROSS JOIN (SELECT dropoff_weekday from calc.ride_request_data_calc group by dropoff_weekday) as B
CROSS JOIN (SELECT dropoff_hour from calc.ride_request_data_calc group by dropoff_hour) as C),

profile as
(SELECT dropoff_grid
		,dropoff_weekday
		,dropoff_hour
		,dropoff_cnt_month
		,dropoff_cnt_weekday
		,dropoff_cnt_hour
		,grid_total_prop
		,weekday_prop
		,hour_prop
		,ROUND(CAST(avg(reward) AS NUMERIC),2) as reward_avg
		,ROUND(CAST(avg(duration) AS NUMERIC),2) as duration_avg
		,ROUND(CAST(avg(distance) AS NUMERIC),2) as distance_avg
FROM 
(SELECT dropoff_grid
		,dropoff_weekday
		,dropoff_hour
		,count(*) over (partition by dropoff_grid) as dropoff_cnt_month
		,count(*) over (partition by dropoff_grid, dropoff_weekday) as dropoff_cnt_weekday
		,count(*) over (partition by dropoff_grid, dropoff_weekday, dropoff_hour) as dropoff_cnt_hour
 		,cast(count(*) over (partition by dropoff_grid) as numeric) / count(*) over () as grid_total_prop
 		,cast(count(*) over (partition by dropoff_grid, dropoff_weekday) as numeric) / count(*) over (partition by dropoff_weekday) as weekday_prop
 		,cast(count(*) over (partition by dropoff_grid, dropoff_weekday, dropoff_hour) as numeric) / count(*) over (partition by dropoff_weekday, dropoff_hour) as hour_prop
		,reward
 		,distance
 		,duration
	FROM calc.ride_request_data_calc) as t
GROUP BY dropoff_grid
		,dropoff_weekday
		,dropoff_hour
		,dropoff_cnt_month
		,dropoff_cnt_weekday
		,dropoff_cnt_hour
		,grid_total_prop
		,weekday_prop
		,hour_prop)
 
SELECT fullgrid.dropoff_grid
		,fullgrid.dropoff_weekday
		,fullgrid.dropoff_hour
		,COALESCE(A.dropoff_cnt_month,0) as dropoff_cnt_month
		,COALESCE(B.dropoff_cnt_weekday,0) as dropoff_cnt_weekday
		,COALESCE(profile.dropoff_cnt_hour,0) as dropoff_cnt_hour
		,COALESCE(profile.grid_total_prop,0) as grid_total_prop
		,COALESCE(profile.weekday_prop,0) as weekday_prop
		,COALESCE(profile.hour_prop,0) as hour_prop
		,reward_avg
		,duration_avg
		,distance_avg
from fullgrid
LEFT JOIN (
	SELECT dropoff_grid
		,dropoff_cnt_month
	FROM PROFILE
	GROUP BY dropoff_grid
		,dropoff_cnt_month
	) AS A ON A.dropoff_grid = fullgrid.dropoff_grid
LEFT JOIN (
	SELECT dropoff_grid
		,dropoff_weekday
		,dropoff_cnt_weekday
	FROM PROFILE
	GROUP BY dropoff_grid
		,dropoff_weekday
		,dropoff_cnt_weekday
	) AS B ON B.dropoff_grid = fullgrid.dropoff_grid
	AND B.dropoff_weekday = fullgrid.dropoff_weekday
LEFT JOIN PROFILE ON fullgrid.dropoff_grid = PROFILE.dropoff_grid
	AND fullgrid.dropoff_weekday = PROFILE.dropoff_weekday
	AND fullgrid.dropoff_hour = PROFILE.dropoff_hour
