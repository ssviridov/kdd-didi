WITH tab as (
    SELECT order_id
         , driver_id
         , ST_DistanceSphere(pickup_point,
                             lag(dropoff_point) over (partition by driver_id order by ride_start_time)) AS pickup_distance
         , extract(epoch from ride_start_time - lag(ride_stop_time)
                                                over (partition by driver_id order by ride_start_time)) AS pickup_time
         , ST_DistanceSphere(pickup_point,
                             lag(dropoff_point) over (partition by driver_id order by ride_start_time)) /
           extract(epoch from ride_start_time - lag(ride_stop_time)
                                                over (partition by driver_id order by ride_start_time)) AS pickup_speed
    FROM calc.map_order_driver
             JOIN geo.ride_request_data using (order_id))
select *
FROM tab
where pickup_speed > 3 and pickup_speed < 20;