import os

import pandas as pd
import numpy as np

import logging

logger = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(__file__))
path_df_lambda = os.path.join(cur_dir, "src", "driver_lambdas.csv")
path_df_grid = os.path.join(cur_dir, "src", "driver_grid_probs.csv")
path_hexes = os.path.join(cur_dir, "..", "..", "data", "hexes.csv")


class DriverGenerator:
    def __init__(self, path_df_lambda=path_df_lambda, path_df_probs=path_df_grid, path_hexes=path_hexes,
                 random_seed=None):
        logger.info("Initialize driver generator")
        self.df_lambda = pd.read_csv(path_df_lambda, sep=";").dropna()
        self.df_probs = pd.read_csv(path_df_probs, sep=";").dropna()
        hexes = pd.read_csv(path_hexes)
        self.df_probs = self.df_probs.loc[self.df_probs["first_grid"].isin(hexes["hex"])]
        if random_seed:
            np.random.seed(random_seed)

    def determine_lambdas(self, weekday, hour):
        res = self.df_lambda.loc[(self.df_lambda["pickup_weekday"] == weekday)
                                 & (self.df_lambda["appearance"] == hour),
                                 ["driver_cnt_avg", "lifetime_avg"]]
        return res.values[0]

    def determine_grid_probs(self, weekday, hour):
        res = self.df_probs.loc[(self.df_probs["pickup_weekday"] == weekday)
                                & (self.df_probs["appearance"] == hour),
                                ["first_grid", "grid_cnt"]]
        return res["first_grid"].values, res["grid_cnt"].values / res["grid_cnt"].values.sum()

    def generate_drivers(self, weekday: int):
        assert (weekday >= 1) and (weekday <= 7)

        start = 0
        d_drivers = {}
        for h in range(24):
            cur_time = start + h * 3600
            driver_amt, lifetime = self.determine_lambdas(weekday=weekday, hour=h)
            driver_amt = int(driver_amt)
            grids, weights = self.determine_grid_probs(weekday=weekday, hour=h)
            t = np.random.exponential(scale=1 / driver_amt, size=driver_amt)
            t_cum = np.cumsum(t)
            driver_time = [cur_time + self.hour_to_seconds(h) for h in t_cum]
            driver_lifetime = np.random.exponential(lifetime, size=driver_amt) * 60
            idx = np.random.choice(np.arange(grids.shape[0]), size=driver_amt, p=weights)
            driver_grid = grids[idx]
            for k, v1, v2 in zip(driver_time, driver_grid, driver_lifetime.round()):
                d_drivers.setdefault(k, []).append((v1, v2))
        return d_drivers

    @staticmethod
    def hour_to_seconds(h):
        """
        Convert hour to seconds, round them, and make +1
        """
        return int(h * 3600) + 1


if __name__ == "__main__":
    driver_gen = DriverGenerator()
    res = driver_gen.generate_drivers(weekday=1)
