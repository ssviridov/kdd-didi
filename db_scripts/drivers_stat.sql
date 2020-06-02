-- driver lambdas
with J as
         (select *
               , CASE
                     WHEN extract(epoch from ride_start_time
                         - lag(ride_stop_time)
                           over (partition by driver_id order by ride_start_time)) < 3600
                         THEN 0
                     ELSE 1
                 END group_ind
          from calc.map_order_driver
                   join calc.ride_request_data_calc using (order_id)),
     D as (select *, sum(group_ind) OVER (PARTITION BY driver_id ORDER BY ride_start_time) AS group_id
           from J),
     H as (select EXTRACT(epoch FROM max(ride_stop_time) - min(ride_start_time)) / 60 as lifetime
                , min(pickup_hour)                                                    as appearance
                , count(*)                                                            as orders_amt
                , sum(duration)                                                       as total_duration
                , min(EXTRACT(DAY FROM ride_start_time))                              as pickup_day
                , min(pickup_weekday)                                                 as pickup_weekday
           from D
           group by driver_id, group_id),
     tab as (SELECT pickup_weekday
                  , appearance
                  , count(*)      as driver_cnt
                  , avg(lifetime) as lifetime
             FROM H
             group by pickup_weekday, appearance, pickup_day)
SELECT pickup_weekday
     , appearance
     , avg(driver_cnt) as driver_cnt_avg
     , avg(lifetime)   as lifetime_avg
FROM tab
group by pickup_weekday, appearance;

-- driver grid probs
with J as
         (select *
               , CASE
                     WHEN extract(epoch from ride_start_time
                         - lag(ride_stop_time)
                           over (partition by driver_id order by ride_start_time)) < 3600
                         THEN 0
                     ELSE 1
                 END group_ind
          from calc.map_order_driver
                   join calc.ride_request_data_calc using (order_id)),
     D as (select *, sum(group_ind) OVER (PARTITION BY driver_id ORDER BY ride_start_time) AS group_id
           from J),
     H as (select *,
                  first_value(pickup_grid) over
                      (partition by driver_id, group_id order by ride_start_time) as first_grid
           from D),
     tab as (select min(pickup_weekday) as pickup_weekday
                  , min(pickup_hour) as appearance
                  , min(first_grid)  as first_grid
             from H
             group by driver_id, group_id)
SELECT pickup_weekday
     , appearance
     , first_grid
     , count(*) as grid_cnt
FROM tab
group by pickup_weekday, appearance, first_grid;

