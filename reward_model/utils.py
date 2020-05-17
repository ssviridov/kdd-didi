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
