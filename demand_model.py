import os

import pandas as pd
import numpy as np
from numpy import random
from datetime import timedelta, datetime


class RequestModel:
    def __init__(self, path_to_df_lambda, path_to_df_probs, random_seed=None, start_time=datetime(2016, 11, 1),
                 end_time=datetime(2016, 12, 1)):
        self.df_lambda = pd.read_csv(path_to_df_lambda, sep=";")
        self.df_probs = pd.read_csv(path_to_df_probs, sep=";")
        self.grids = self.df_lambda["pickup_grid"].unique()

        if random_seed:
            random.seed(random_seed)

        self.start_time = start_time
        self.end_time = end_time

    def determine_lambda(self, pickup_grid, pickup_time):
        weekend = pickup_time.weekday() // 5
        hour = pickup_time.hour
        res = self.df_lambda.loc[(self.df_lambda["pickup_grid"] == pickup_grid)
                                 & (self.df_lambda["pickup_weekend"] == weekend)
                                 & (self.df_lambda["pickup_hour"] == hour),
                                 "pickup_cnt_avg"]
        if res.empty:
            return 0
        else:
            return res.values[0]

    def determine_dropoff_probs(self, grid_destination, pickup_time):
        weekend = pickup_time.weekday() // 5
        hour = pickup_time.hour
        res = grid_destination.loc[(grid_destination["pickup_weekend"] == weekend)
                                   & (grid_destination["pickup_hour"] == hour),
                                   ["dropoff_grid", "pickup_cnt"]]
        return res["dropoff_grid"].values, res["pickup_cnt"].values / res["pickup_cnt"].values.sum()

    def one_grid_requests(self, pickup_grid, start=None, finish=None):
        assert (start.minute == 0) & (finish.minute == 0)
        grid_destination = self.df_probs.loc[self.df_probs["pickup_grid"] == pickup_grid]
        request_time = []
        request_pickup = []
        request_dropoff = []
        if not start:
            start = self.start_time
        if not finish:
            finish = self.end_time

        total_hours = int((finish - start).total_seconds() / 3600)
        for i in range(total_hours):
            cur_time = start + timedelta(hours=i)
            lambd = self.determine_lambda(pickup_grid, cur_time)
            destinations, weights = self.determine_dropoff_probs(grid_destination, cur_time)
            if lambd == 0:  # go to next hour
                continue
            else:
                t = random.exponential(scale=1 / lambd, size=int(2 * lambd))
                t_cum = np.cumsum(t)
                n_req = np.argmin(t_cum < 1)
                request_time += [cur_time + timedelta(hours=h) for h in t_cum[:n_req]]
                request_pickup += [pickup_grid] * n_req
                request_dropoff += random.choice(destinations, size=n_req, p=weights).tolist()
        return request_time, request_pickup, request_dropoff

    def all_grid_requests(self, start=None, finish=None):
        all_request_time = []
        all_request_pickup = []
        all_request_dropoff = []
        start_method = datetime.now()
        n_grids = len(self.grids)
        for i, grid in enumerate(self.grids, 1):
            start_loop = datetime.now()
            request_time, request_pickup, request_dropoff = self.one_grid_requests(grid, start, finish)
            all_request_time += request_time
            all_request_pickup += request_pickup
            all_request_dropoff += request_dropoff
            finish_loop = datetime.now()
            total_time = (finish_loop - start_method).total_seconds()
            loop_time = (finish_loop - start_loop).total_seconds()
            estimated_time = n_grids * total_time / i
            print(
                f"Done {i}/{n_grids}. Loop time: {loop_time:.1f}. Total time: {total_time:.1f}. Estimated time: {estimated_time:.1f}")
        return all_request_time, all_request_pickup, all_request_dropoff


if __name__ == "__main__":
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    path_df_lambda = os.path.join(cur_dir, "data", "pickup_cnt.csv")
    path_df_destination = os.path.join(cur_dir, "data", "pick_drop.csv")
    random_seed = 42
    start = datetime(2016, 11, 1)
    finish = datetime(2016, 11, 2)

    rm = RequestModel(path_df_lambda, path_df_destination, random_seed=random_seed)
    all_request_time, all_request_pickup, all_request_dropoff = rm.all_grid_requests(start=start, finish=finish)
