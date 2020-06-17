DROP TABLE IF EXISTS calc.pickup_ranks;
CREATE TABLE calc.pickup_ranks AS

SELECT pickup_grid
	,pickup_weekday
	,pickup_hour
	,(pickup_prop - min_prop) / (max_prop - min_prop) AS pickup_rank
FROM (
	SELECT pickup_grid
		,pickup_weekday
		,pickup_hour
		,CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg AS pickup_prop
		,min(CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg) OVER (PARTITION BY pickup_weekday,pickup_hour) AS min_prop
		,max(CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg) OVER (PARTITION BY pickup_weekday,pickup_hour) AS max_prop
	FROM (
		SELECT A.pickup_grid
			,A.pickup_weekday
			,A.pickup_hour
			,count(*) OVER (PARTITION BY A.pickup_grid,A.pickup_weekday,A.pickup_hour) / B.days_uniques AS grid_hour_avg
			,count(*) OVER (PARTITION BY A.pickup_weekday,A.pickup_hour) / B.days_uniques AS weekday_hour_avg
		FROM calc.ride_request_data_calc AS A
		LEFT JOIN (
			SELECT pickup_weekday
				,count(DISTINCT extract(day FROM ride_start_time)) AS days_uniques
			FROM calc.ride_request_data_calc
			GROUP BY pickup_weekday
			) AS B ON A.pickup_weekday = B.pickup_weekday
		) AS t
	) AS t1
GROUP BY pickup_grid
	,pickup_weekday
	,pickup_hour
	,pickup_rank
