DROP TABLE IF EXISTS calc.grids_ranks;
CREATE TABLE calc.grids_ranks as

	WITH all_grids
AS (
	SELECT DISTINCT grid_id
		,day_of_week
		,hour
	FROM (
		SELECT pickup_grid AS grid_id
			,pickup_weekday AS day_of_week
			,pickup_hour AS hour
		FROM calc.pickup_ranks
		GROUP BY pickup_grid
			,pickup_weekday
			,pickup_hour
		
		UNION ALL
		
		SELECT dropoff_grid AS grid_id
			,dropoff_weekday AS day_of_week
			,dropoff_hour AS hour
		FROM calc.dropoff_ranks
		GROUP BY dropoff_grid
			,dropoff_weekday
			,dropoff_hour
		) AS t
	)
SELECT grid_id
	,day_of_week
	,hour
	,COALESCE(B.pickup_rank, 0) AS pickup_rank
	,COALESCE(C.dropoff_rank, 0) AS dropoff_rank
FROM all_grids AS A
LEFT JOIN calc.pickup_ranks AS B ON A.grid_id = B.pickup_grid
	AND A.day_of_week = B.pickup_weekday
	AND A.hour = B.pickup_hour
LEFT JOIN calc.dropoff_ranks AS C ON A.grid_id = C.dropoff_grid
	AND A.day_of_week = C.dropoff_weekday
	AND A.hour = C.dropoff_hour
