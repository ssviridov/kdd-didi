DROP TABLE IF EXISTS calc.ride_request_data_calc;

CREATE TABLE calc.ride_request_data_calc as
WITH A as (
select order_id
, ride_start_time  + interval '8h' as ride_start_time
, ride_stop_time + interval '8h' as ride_stop_time
, pickup_point
, dropoff_point
, reward
from geo.ride_request_data
)
SELECT A.order_id
	,A.ride_start_time
	,CASE 
		WHEN EXTRACT(dow FROM A.ride_start_time) = 0
			THEN 7
		ELSE EXTRACT(dow FROM A.ride_start_time)
		END pickup_weekday
	,EXTRACT(hour FROM A.ride_start_time) AS pickup_hour
	,A.ride_stop_time
	,CASE 
		WHEN EXTRACT(dow FROM A.ride_stop_time) = 0
			THEN 7
		ELSE EXTRACT(dow FROM A.ride_stop_time)
		END dropoff_weekday
	,EXTRACT(hour FROM A.ride_stop_time) AS dropoff_hour
	,A.pickup_point
	,B.grid_id AS pickup_grid
	,A.dropoff_point
	,C.grid_id AS dropoff_grid
	,A.reward
	,ST_DistanceSphere(A.pickup_point, A.dropoff_point) / 1000 AS distance
	,ROUND(CAST(EXTRACT(hour FROM (A.ride_stop_time - A.ride_start_time)) * 60 + EXTRACT(minute FROM (A.ride_stop_time - A.ride_start_time)) + EXTRACT(second FROM (A.ride_stop_time - A.ride_start_time)) / 60 AS NUMERIC), 2) AS duration
FROM A
LEFT JOIN geo.hexagon_grid_data AS B ON ST_Contains(B.polygon, A.pickup_point)
LEFT JOIN geo.hexagon_grid_data AS C ON ST_Contains(C.polygon, A.dropoff_point);
