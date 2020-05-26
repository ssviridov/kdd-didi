import pandas as pd
from scipy import stats


class CancelModel:
    def __init__(self, bw=0.005, data_path="cancel_probs_distr.csv"):
        """

        Parameters
        ----------
        bw :
        data_path : path to DataFrame - Should contain only prob_cols!!!
        """
        df = pd.read_csv(data_path)
        data = df.values.T
        self.kernel = stats.gaussian_kde(data, bw_method=bw)

    def sample_probs(self, size=1):
        return self.kernel.resample(size)


if __name__ == "__main__":
    model = CancelModel()
    print(model.sample_probs(1))
