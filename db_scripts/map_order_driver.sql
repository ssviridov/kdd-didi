DROP TABLE IF EXISTS calc.map_order_driver;

CREATE TABLE calc.map_order_driver as
SELECT DISTINCT driver_id, order_id
FROM geo.trajectory_data;
