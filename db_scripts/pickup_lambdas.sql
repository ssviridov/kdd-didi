with tab as (SELECT pickup_grid
                  , EXTRACT(DAY FROM ride_start_time)              as pickup_day
                  , Case when pickup_weekday > 5 then 1 else 0 end as pickup_weekend
                  , pickup_hour
                  , count(*)                                       as pickup_cnt
             FROM calc.ride_request_data_calc
             group by pickup_grid, pickup_weekday, pickup_hour, EXTRACT(DAY FROM ride_start_time))
SELECT pickup_grid
     , pickup_weekend
     , pickup_hour
     , avg(pickup_cnt) as pickup_cnt_avg
FROM tab
group by pickup_grid, pickup_weekend, pickup_hour