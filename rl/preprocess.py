import pandas as pd
from utils import time_to_sincos


def simple_preprocess(df):
    df['ride_start_time'] = df.ride_start_time.apply(lambda x: pd.Timestamp(x))

    df['pickup_seconds'] = df['ride_start_time'].dt.hour * 3600 + df['ride_start_time'].dt.minute * 60 + df['ride_start_time'].dt.second

    df['ride_stop_time'] = df.ride_stop_time.apply(lambda x: pd.Timestamp(x))

    df['dropoff_seconds'] = df['ride_stop_time'].dt.hour * 3600 + df['ride_stop_time'].dt.minute * 60 + df['ride_stop_time'].dt.second

    df['pickup_second_sin'], df['pickup_second_cos'] = time_to_sincos(df['pickup_seconds'], value_type='second')
    df['dropoff_second_sin'], df['dropoff_second_cos'] = time_to_sincos(df['dropoff_seconds'], value_type='second')

    df['day_of_week'] = df['ride_start_time'].dt.weekday
    df['day_of_week_sin'], df['day_of_week_cos'] = time_to_sincos(df['day_of_week'], value_type='day_of_week')
    df['day_of_month'] = df['ride_start_time'].dt.month
    df['day_of_month_sin'], df['day_of_month_cos'] = time_to_sincos(df['day_of_month'], value_type='day_of_month')
    # df['pickup_hour_sin'], df['pickup_hour_cos'] = time_to_sincos(df['pickup_hour'], value_type='hour')
    # df['pickup_weekday_sin'], df['pickup_weekday_cos'] = time_to_sincos(df['pickup_weekday'], value_type='day_of_week')
    #
    # df['dropoff_hour_sin'], df['dropoff_hour_cos'] = time_to_sincos(df['dropoff_hour'], value_type='hour')
    # df['dropoff_weekday_sin'], df['dropoff_weekday_cos'] = time_to_sincos(df['dropoff_weekday'],
    #                                                                       value_type='day_of_week')
    df['info'] = (df.ride_stop_time - df.ride_start_time).apply(lambda x: x.seconds) / 60 / 10
    return df
