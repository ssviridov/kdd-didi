with tab as (SELECT pickup_weekday
                  , pickup_hour
                  , count(*)                                       as pickup_cnt
             FROM calc.ride_request_data_calc
             group by pickup_weekday, pickup_hour, EXTRACT(DAY FROM ride_start_time))
SELECT pickup_weekday
     , pickup_hour
     , avg(pickup_cnt) as pickup_cnt_avg
FROM tab
group by pickup_weekday, pickup_hour