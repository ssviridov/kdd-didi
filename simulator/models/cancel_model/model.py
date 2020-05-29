import os

import pandas as pd
from scipy import stats

import logging

logger = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(cur_dir, "src")


class CancelModel:
    def __init__(self, bw=0.005, data_dir=data_dir, weekday=1):
        """
        data_path : path to DataFrame - Should contain only prob_cols!!!
        """
        logger.info("Initialize cancellation model")
        data_path = os.path.join(data_dir, f"cancel_probs_day{weekday}.csv.gz")
        df = pd.read_csv(data_path, compression="gzip")
        data = df.values.T
        self.kernel = stats.gaussian_kde(data, bw_method=bw)

    def sample_probs(self, size=1):
        return self.kernel.resample(size)


if __name__ == "__main__":
    model = CancelModel()
    print(model.sample_probs(1))
