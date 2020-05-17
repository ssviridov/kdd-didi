DROP TABLE IF EXISTS calc.pickups_profile;
CREATE TABLE calc.pickups_profile as
WITH fullgrid as
(select A.pickup_grid
		,B.pickup_weekday
		,C.pickup_hour
FROM (SELECT pickup_grid from calc.ride_request_data_calc group by pickup_grid) as A
CROSS JOIN (SELECT pickup_weekday from calc.ride_request_data_calc group by pickup_weekday) as B
CROSS JOIN (SELECT pickup_hour from calc.ride_request_data_calc group by pickup_hour) as C),

profile as
(SELECT pickup_grid
		,pickup_weekday
		,pickup_hour
		,pickup_cnt_month
		,pickup_cnt_weekday
		,pickup_cnt_hour
		,grid_total_prop
		,weekday_prop
		,hour_prop
		,ROUND(CAST(avg(reward) AS NUMERIC),2) as reward_avg
		,ROUND(CAST(avg(duration) AS NUMERIC),2) as duration_avg
		,ROUND(CAST(avg(distance) AS NUMERIC),2) as distance_avg
FROM 
(SELECT pickup_grid
		,pickup_weekday
		,pickup_hour
		,count(*) over (partition by pickup_grid) as pickup_cnt_month
		,count(*) over (partition by pickup_grid, pickup_weekday) as pickup_cnt_weekday
		,count(*) over (partition by pickup_grid, pickup_weekday, pickup_hour) as pickup_cnt_hour
 		,cast(count(*) over (partition by pickup_grid) as numeric) / count(*) over () as grid_total_prop
 		,cast(count(*) over (partition by pickup_grid, pickup_weekday) as numeric) / count(*) over (partition by pickup_weekday) as weekday_prop
 		,cast(count(*) over (partition by pickup_grid, pickup_weekday, pickup_hour) as numeric) / count(*) over (partition by pickup_weekday, pickup_hour) as hour_prop
		,reward
 		,distance
 		,duration
	FROM calc.ride_request_data_calc) as t
GROUP BY pickup_grid
		,pickup_weekday
		,pickup_hour
		,pickup_cnt_month
		,pickup_cnt_weekday
		,pickup_cnt_hour
		,grid_total_prop
		,weekday_prop
		,hour_prop)
 
SELECT fullgrid.pickup_grid
		,fullgrid.pickup_weekday
		,fullgrid.pickup_hour
		,COALESCE(profile.pickup_cnt_month,0) as pickup_cnt_month
		,COALESCE(profile.pickup_cnt_weekday,0) as pickup_cnt_weekday
		,COALESCE(profile.pickup_cnt_hour,0) as pickup_cnt_hour
		,COALESCE(profile.grid_total_prop,0) as grid_total_prop
		,COALESCE(profile.weekday_prop,0) as weekday_prop
		,COALESCE(profile.hour_prop,0) as hour_prop
		,reward_avg
		,duration_avg
		,distance_avg
from fullgrid
left join profile
on fullgrid.pickup_grid = profile.pickup_grid
and fullgrid.pickup_weekday = profile.pickup_weekday
and fullgrid.pickup_hour = profile.pickup_hour