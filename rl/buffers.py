import random
import os
import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from collections import deque
from simulator.utils import DataManager
from utils import time_to_sincos

cur_dir = os.path.dirname(os.path.abspath(__file__))


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
        # TODO add 'done' array
        done = np.zeros(reward.shape)
        return state, action, reward, next_state, done

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
        # TODO create 'done' array
        done = np.zeros(reward.shape)
        return state, reward, next_state, info, done

    @staticmethod
    def preprocess(record):
        # prepare state feature
        pickup_hour_sin, pickup_hour_cos = time_to_sincos(record["pickup_hour"], value_type='hour')
        pickup_weekday_sin, pickup_weekday_cos = time_to_sincos(record["pickup_weekday"], value_type='day_of_week')

        state = [pickup_hour_sin,
                 pickup_hour_cos,
                 pickup_weekday_sin,
                 pickup_weekday_cos,
                 record["pickup_lon"],
                 record["pickup_lat"]]

        dropoff_hour_sin, dropoff_hour_cos = time_to_sincos(record["dropoff_hour"], value_type='hour')
        dropoff_weekday_sin, dropoff_weekday_cos = time_to_sincos(record["dropoff_weekday"], value_type='day_of_week')

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
        if self.db_client.training_collection.count() < batch_size:
            samples = self.db_client.trajectories_collection.aggregate([{"$sample": {"size": batch_size}}])
        else:
            samples = self.db_client.training_collection.aggregate([{"$sample": {"size": batch_size}}])
            self.db_client.truncate_training_collection()
        state, reward, new_state, info, done = self._prepare_samples(list(samples))
        return state, reward, new_state, info, done

    @staticmethod
    def _prepare_samples(samples: list):
        reward_list, info_list, state_list, new_state_list, done_list = list(), list(), list(), list(), list()
        for sample in samples:
            reward_list.append(sample['reward'])
            info_list.append(sample['t_end'] - sample['t_start'] + 1)
            done_list.append(sample['done'])

            pickup_hour_sin, pickup_hour_cos = time_to_sincos(int(sample["t_start"] / (60 * 60)), value_type='hour')
            pickup_weekday_sin, pickup_weekday_cos = time_to_sincos(sample["day_of_week"], value_type='day_of_week')
            state = [pickup_hour_sin,
                     pickup_hour_cos,
                     pickup_weekday_sin,
                     pickup_weekday_cos,
                     sample["lonlat_start"][0],
                     sample["lonlat_start"][1]]
            state_list.append(state)

            dropoff_hour_sin, dropoff_hour_cos = time_to_sincos(int(sample["t_end"] / (60 * 60)), value_type='hour')
            dropoff_weekday_sin, dropoff_weekday_cos = time_to_sincos(sample["day_of_week"], value_type='day_of_week')
            new_state = [dropoff_hour_sin,
                         dropoff_hour_cos,
                         dropoff_weekday_sin,
                         dropoff_weekday_cos,
                         sample["lonlat_end"][0],
                         sample["lonlat_end"][1]]
            new_state_list.append(new_state)

        return np.array(state_list), np.array(reward_list), np.array(new_state_list), np.array(info_list), np.array(
            done_list)


class MongoBufferRanks(MongoDBReplayBuffer):
    def __init__(self):
        with open(os.path.join(cur_dir, 'data', 'grids_ranks.json'), 'r') as f:
            self.grids_ranks = json.load(f)
        super().__init__()

    def _prepare_samples(self, samples: list):
        reward_list, info_list, state_list, new_state_list, done_list = list(), list(), list(), list(), list()
        for sample in samples:
            reward_list.append(sample['reward'])
            info_list.append(sample['t_end'] - sample['t_start'] + 1)
            done_list.append(sample['done'])

            start_hour = int(sample["t_start"] / (60 * 60))
            pickup_hour_sin, pickup_hour_cos = time_to_sincos(start_hour, value_type='hour')
            pickup_weekday_sin, pickup_weekday_cos = time_to_sincos(sample["day_of_week"], value_type='day_of_week')
            start_pickup_rank, start_dropoff_rank = self._get_grid_ranks(sample['hex_start'], sample['day_of_week'],
                                                                         int(sample["t_start"] / (60 * 60)))
            start_minute = int(sample["t_start"] / 60)
            start_minute_5 = int(sample["t_start"] / 60 / 5)
            start_minute_10 = int(sample["t_start"] / 60 / 10)

            state = [pickup_hour_sin,
                     pickup_hour_cos,
                     pickup_weekday_sin,
                     pickup_weekday_cos,
                     sample["lonlat_start"][0],
                     sample["lonlat_start"][1],
                     start_pickup_rank,
                     start_dropoff_rank,
                     start_minute,
                     start_minute_5,
                     start_minute_10]
            state_list.append(state)

            dropoff_hour_sin, dropoff_hour_cos = time_to_sincos(int(sample["t_end"] / (60 * 60)), value_type='hour')
            dropoff_weekday_sin, dropoff_weekday_cos = time_to_sincos(sample["day_of_week"], value_type='day_of_week')
            end_pickup_rank, end_dropoff_rank = self._get_grid_ranks(sample['hex_end'], sample['day_of_week'],
                                                                     int(sample["t_end"] / (60 * 60)))
            end_minute = int(sample["t_end"] / 60)
            end_minute_5 = int(sample["t_end"] / 60 / 5)
            end_minute_10 = int(sample["t_end"] / 60 / 10)

            new_state = [dropoff_hour_sin,
                         dropoff_hour_cos,
                         dropoff_weekday_sin,
                         dropoff_weekday_cos,
                         sample["lonlat_end"][0],
                         sample["lonlat_end"][1],
                         end_pickup_rank,
                         end_dropoff_rank,
                         end_minute,
                         end_minute_5,
                         end_minute_10]
            new_state_list.append(new_state)

        return np.array(state_list), np.array(reward_list), np.array(new_state_list), np.array(info_list), np.array(done_list)

    def _get_grid_ranks(self, hex_id, day_of_week, hour):
        idx = hex_id + '_' + str(day_of_week) + '_' + str(hour)
        ranks = self.grids_ranks.get(idx, {'pickup_rank': 0, 'dropoff_rank': 0})
        return ranks['pickup_rank'], ranks['dropoff_rank']


# if __name__ == "__main__":
#     from pprint import pprint
#
#     test = MongoBufferRanks()
#     pprint(len(test))
#     pprint(test.sample(batch_size=10))
