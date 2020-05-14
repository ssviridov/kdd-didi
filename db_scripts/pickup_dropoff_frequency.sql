SELECT pickup_grid
     , dropoff_grid
     , Case when pickup_weekday > 5 then 1 else 0 end as pickup_weekend
     , pickup_hour
     , count(*)                                       as pickup_cnt
FROM calc.ride_request_data_calc
group by pickup_grid, dropoff_grid, Case when pickup_weekday > 5 then 1 else 0 end, pickup_hour