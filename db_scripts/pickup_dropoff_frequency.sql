SELECT pickup_grid
     , dropoff_grid
     , pickup_weekday
     , pickup_hour
     , count(*)                                       as pickup_cnt
FROM calc.ride_request_data_calc
group by pickup_grid, dropoff_grid, pickup_weekday, pickup_hour