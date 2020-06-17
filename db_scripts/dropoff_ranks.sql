DROP TABLE IF EXISTS calc.dropoff_ranks;
CREATE TABLE calc.dropoff_ranks AS

SELECT dropoff_grid
	,dropoff_weekday
	,dropoff_hour
	,(dropoff_prop - min_prop) / (max_prop - min_prop) AS dropoff_rank
FROM (
	SELECT dropoff_grid
		,dropoff_weekday
		,dropoff_hour
		,CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg AS dropoff_prop
		,min(CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg) OVER (PARTITION BY dropoff_weekday,dropoff_hour) AS min_prop
		,max(CAST(grid_hour_avg AS DECIMAL) / weekday_hour_avg) OVER (PARTITION BY dropoff_weekday,dropoff_hour) AS max_prop
	FROM (
		SELECT A.dropoff_grid
			,A.dropoff_weekday
			,A.dropoff_hour
			,count(*) OVER (PARTITION BY A.dropoff_grid,A.dropoff_weekday,A.dropoff_hour) / B.days_uniques AS grid_hour_avg
			,count(*) OVER (PARTITION BY A.dropoff_weekday,A.dropoff_hour) / B.days_uniques AS weekday_hour_avg
		FROM calc.ride_request_data_calc AS A
		LEFT JOIN (
			SELECT dropoff_weekday
				,count(DISTINCT extract(day FROM ride_start_time)) AS days_uniques
			FROM calc.ride_request_data_calc
			GROUP BY dropoff_weekday
			) AS B ON A.dropoff_weekday = B.dropoff_weekday
		) AS t
	) AS t1
GROUP BY dropoff_grid
	,dropoff_weekday
	,dropoff_hour
	,dropoff_rank
