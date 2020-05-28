import os

import pandas as pd
from scipy import stats


class CancelModel:
    def __init__(self, bw=0.005, data_path="cancel_probs_distr.csv"):
        """
        data_path : path to DataFrame - Should contain only prob_cols!!!
        """
        df = pd.read_csv(data_path)
        data = df.values.T
        self.kernel = stats.gaussian_kde(data, bw_method=bw)

    def sample_probs(self, size=1):
        return self.kernel.resample(size)


if __name__ == "__main__":
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(cur_dir, "src", "cancel_probs_distr.csv")
    model = CancelModel(data_path=data_path)
    print(model.sample_probs(1))