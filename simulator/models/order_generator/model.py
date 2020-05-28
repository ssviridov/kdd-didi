import os

import pandas as pd
import numpy as np
from datetime import timedelta

cur_dir = os.path.dirname(os.path.abspath(__file__))
path_df_lambda = os.path.join(cur_dir, "src", "avg_hourly_orders.csv")
path_df_destination = os.path.join(cur_dir, "src", "correspondence_frequency.csv")


class OrderGenerator:
    def __init__(self, path_to_df_lambda=path_df_lambda, path_to_df_probs=path_df_destination, random_seed=None):
        self.df_lambda = pd.read_csv(path_to_df_lambda, sep=";").dropna()
        self.df_probs = pd.read_csv(path_to_df_probs, sep=";")
        if random_seed:
            np.random.seed(random_seed)

    def determine_lambda(self, weekday, hour):
        res = self.df_lambda.loc[(self.df_lambda["pickup_weekday"] == weekday)
                                 & (self.df_lambda["pickup_hour"] == hour),
                                 "pickup_cnt_avg"]
        if res.empty:
            return 0
        else:
            return res.values[0]

    def determine_correspondence_probs(self, weekday, hour):
        res = self.df_probs.loc[(self.df_probs["pickup_weekday"] == weekday)
                                & (self.df_probs["pickup_hour"] == hour),
                                ["pickup_grid", "dropoff_grid", "pickup_cnt"]]
        return res[["pickup_grid", "dropoff_grid"]].values, res["pickup_cnt"].values / res["pickup_cnt"].values.sum()

    def generate_orders(self, weekday: int):
        assert (weekday >= 1) and (weekday <= 7)

        start = timedelta(0)
        df_list = []
        for h in range(24):
            cur_time = start + timedelta(hours=h)
            lambd = self.determine_lambda(weekday=weekday, hour=h)
            correspondences, weights = self.determine_correspondence_probs(weekday=weekday, hour=h)
            if lambd == 0:  # go to next hour
                continue
            else:
                t = np.random.exponential(scale=1 / lambd, size=int(2 * lambd))
                t_cum = np.cumsum(t)
                n_req = np.argmin(t_cum < 1)
                order_time = [cur_time + timedelta(seconds=self.hour_to_seconds(h)) for h in t_cum[:n_req]]
                idx = np.random.choice(np.arange(correspondences.shape[0]), size=n_req, p=weights)
                order_correspondence = correspondences[idx]
                _df = pd.DataFrame(order_correspondence, columns=["pickup_hex", "dropoff_hex"])
                _df["order_time"] = order_time
                df_list.append(_df)
        return pd.concat(df_list, ignore_index=True)

    @staticmethod
    def hour_to_seconds(h):
        """
        Convert hour to seconds, round them, and make +1
        """
        return int(h * 3600) + 1


if __name__ == "__main__":
    order_gen = OrderGenerator()
    res = order_gen.generate_orders(weekday=1)
