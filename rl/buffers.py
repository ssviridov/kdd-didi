import random
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from collections import deque
from simulator.utils import DataManager


class BaseReplayBuffer:

    def sample(self, batch_size):
        pass

    def push(self, record):
        pass

    def load(self, *args):
        pass

    def flush(self):
        pass

    @staticmethod
    def preprocess(record):
        return record

    def __len__(self):
        pass


class CsvReplayBuffer(BaseReplayBuffer):

    def __init__(self):
        self.buffer = None

    def sample(self, batch_size):
        batch = random.sample(self.buffer, batch_size)
        state, action, reward, next_state = map(np.stack, zip(*batch))
        return state, action, reward, next_state

    def push(self, record):
        record = self.preprocess(record)
        self.buffer.append(record)

    def load(self, path):
        pass

    def flush(self):
        self.buffer.clear()

    @staticmethod
    def preprocess(record):
        return record

    def __len__(self):
        return len(self.buffer)


class PostgreSQLReplayBuffer(BaseReplayBuffer):

    def __init__(self,
                 con_str="postgresql://postgres:tb3L2xBBeQCLpkbU@kdd-didi.cyf0lt2tjhid.eu-central-1.rds.amazonaws.com:5432/didi",
                 query="""select ride_start_time,
                        pickup_hour, 
                        pickup_weekday,
                        st_x(pickup_point) as pickup_lon,
                        st_x(pickup_point) as pickup_lat,
                        reward,
                        ride_stop_time,
                        dropoff_hour,
                        dropoff_weekday,
                        st_x(dropoff_point) as dropoff_lon,
                        st_y(dropoff_point) as dropoff_lat
                        from calc.ride_request_data_calc"""):
        self.con = create_engine(con_str)
        self.table = query.split("from")[1].strip()
        self.query = query + " order by random() limit %s"

    def sample(self, batch_size):
        batch = []
        with self.con.connect() as connection:
            sample = connection.execute(self.query % batch_size)
            for row in sample:
                batch.append(self.preprocess(row))
        state, reward, next_state, info = map(np.stack, zip(*batch))
        return state, reward, next_state, info

    @staticmethod
    def preprocess(record):
        # prepare state feature
        pickup_hour_sin = np.sin(record["pickup_hour"] * (2. * np.pi / 23))
        pickup_hour_cos = np.cos(record["pickup_hour"] * (2. * np.pi / 23))
        pickup_weekday_sin = np.sin(record["pickup_weekday"] * (2. * np.pi / 7))
        pickup_weekday_cos = np.cos(record["pickup_weekday"] * (2. * np.pi / 7))

        state = [pickup_hour_sin,
                 pickup_hour_cos,
                 pickup_weekday_sin,
                 pickup_weekday_cos,
                 record["pickup_lon"],
                 record["pickup_lat"]]

        dropoff_hour_sin = np.sin(record["dropoff_hour"] * (2. * np.pi / 23))
        dropoff_hour_cos = np.cos(record["dropoff_hour"] * (2. * np.pi / 23))
        dropoff_weekday_sin = np.sin(record["dropoff_weekday"] * (2. * np.pi / 7))
        dropoff_weekday_cos = np.cos(record["dropoff_weekday"] * (2. * np.pi / 7))

        new_state = [dropoff_hour_sin,
                     dropoff_hour_cos,
                     dropoff_weekday_sin,
                     dropoff_weekday_cos,
                     record["dropoff_lon"],
                     record["dropoff_lat"]]

        info = (record["ride_stop_time"] - record["ride_start_time"]).seconds

        return state, float(record["reward"]), new_state, info

    def __len__(self):
        with self.con.connect() as connection:
            count = connection.execute('select count(*) from %s' % self.table).scalar()
        return count


class MongoDBReplayBuffer(BaseReplayBuffer):
    def __init__(self):
        self.db_client = DataManager()

    def __len__(self):
        return self.db_client.trajectories_collection.count()

    def sample(self, batch_size: int):
        samples = self.db_client.trajectories_collection.aggregate([{"$sample": {"size": batch_size}}])
        return [self._prepare_sample(s) for s in list(samples)]

    @staticmethod
    def _prepare_sample(sample: dict):
        # TODO preprocess sample from MongoDB
        return sample


# if __name__ == "__main__":
#     from pprint import pprint
#     test = MongoDBReplayBuffer()
#     pprint(len(test))
#     pprint(test.sample(batch_size=10))
