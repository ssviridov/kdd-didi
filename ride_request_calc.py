import pandas as pd
import dask.dataframe as ddf
import sys
import os
import datetime
from geopy.distance import geodesic
from sqlalchemy import create_engine

sys.path.append('../')

from coord_to_hexagon import CoordToHex

eng = create_engine(
    'postgresql://postgres:tb3L2xBBeQCLpkbU@kdd-didi.cyf0lt2tjhid.eu-central-1.rds.amazonaws.com:5432/didi')
hex_client = CoordToHex('data/hexagon_grid_table.csv')
PATH_TO_DATA = 'data/total_ride_request/'


def get_info_vec(data):
    dttm_start = pd.to_datetime(data['ride_start_time'].compute(), unit='s')
    dttm_end = pd.to_datetime(data['ride_end_time'].compute(), unit='s')
    data['order_date'] = dttm_start.dt.date
    data['date_hour'] = dttm_start.dt.hour
    data['weekday'] = dttm_start.dt.weekday

    data['distance'] = data.apply(lambda x: geodesic((x.lat_start, x.lon_start), (x.lat_end, x.lon_end)).km, axis=1,
                                  meta=('x', 'float64'))

    data['duration'] = (dttm_end - dttm_start).dt.total_seconds() / 60

    data['start_hex'] = hex_client.get_hex_array(data[['lon_start', 'lat_start']].compute())
    data['end_hex'] = hex_client.get_hex_array(data[['lon_end', 'lat_end']].compute())

    return data


if __name__ == '__main__':
    for file in os.listdir(PATH_TO_DATA):
        print(f'{datetime.datetime.now} - {file} starts')
        sample = ddf.read_csv(PATH_TO_DATA + file, names=['order_id', 'ride_start_time', 'ride_end_time',
                                                          'lon_start', 'lat_start', 'lon_end', 'lat_end', 'reward'])
        sample = sample.reset_index().set_index('index')
        results = get_info_vec(sample).compute()
        print(f'{datetime.datetime.now} - {file} prepared')
        results.to_sql('ride_request_data_calc', eng, schema='calc', if_exists='append', index=False, chunksize=10000)
        print(f'{datetime.datetime.now} - {file} in DB')
