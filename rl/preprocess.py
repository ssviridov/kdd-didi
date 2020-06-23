import pandas as pd
from utils import time_to_sincos


def simple_preprocess(df):
    df['ride_start_time'] = df.ride_start_time.apply(lambda x: pd.Timestamp(x))
    df['ride_stop_time'] = df.ride_stop_time.apply(lambda x: pd.Timestamp(x))
    df['pickup_hour_sin'], df['pickup_hour_cos'] = time_to_sincos(df['pickup_hour'], value_type='hour')
    df['pickup_weekday_sin'], df['pickup_weekday_cos'] = time_to_sincos(df['pickup_weekday'], value_type='day_of_week')

    df['dropoff_hour_sin'], df['dropoff_hour_cos'] = time_to_sincos(df['dropoff_hour'], value_type='hour')
    df['dropoff_weekday_sin'], df['dropoff_weekday_cos'] = time_to_sincos(df['dropoff_weekday'],
                                                                          value_type='day_of_week')
    df['info'] = (df.ride_stop_time - df.ride_start_time).apply(lambda x: x.seconds)
    return df
