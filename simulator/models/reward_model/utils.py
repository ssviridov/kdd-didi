import datetime
import pandas as pd
from geopy.distance import geodesic


def convert_to_datetime(unix_timestamp):
    return datetime.datetime.utcfromtimestamp(int(unix_timestamp)) + datetime.timedelta(hours=8)


def get_distance(start, finish):
    latlon_start = (start[1], start[0])
    latlon_finish = (finish[1], finish[0])
    return geodesic(latlon_start, latlon_finish).km


def generate_dummy(value, values_range, prefix):
    dummies = pd.Series()
    for i in values_range:
        if value == i:
            dummies[prefix + '_' + str(i)] = 1
        else:
            dummies[prefix + '_' + str(i)] = 0
    return dummies


def generate_dummies(df):
    _df = df.copy()
    _df['d_w'] = pd.Categorical(_df['end_dttm'].dt.dayofweek, categories=range(1,8))
    _df['d_h'] = pd.Categorical(_df['end_dttm'].dt.hour, categories=range(0,24))
    _df['p_w'] = pd.Categorical(_df['start_dttm'].dt.dayofweek, categories=range(1,8))
    _df['p_h'] = pd.Categorical(_df['start_dttm'].dt.hour, categories=range(0,24))
    return pd.get_dummies(_df[['d_w', 'd_h', 'p_w', 'p_h']])


def prepare_batch(requests_list: list):
    request_df = pd.DataFrame(requests_list)
    request_df['duration'] = (request_df['order_finish_timestamp'] - request_df['timestamp'] - request_df[
        'pick_up_eta']) / 60
    request_df['start_dttm'] = pd.to_datetime(request_df['timestamp'], unit='s') + pd.to_timedelta(3, 'h')
    request_df['end_dttm'] = pd.to_datetime(request_df['order_finish_timestamp'], unit='s') + pd.to_timedelta(3, 'h')
    return pd.merge(request_df, generate_dummies(request_df), how='left', left_index=True, right_index=True)
