import torch
import pandas as pd
import os
import numpy as np


class CSVDataset(torch.utils.data.Dataset):
    """
    Class for working with static map-style datasets like csv files
        :param: csv_file (string) csv filename
        :param: root_dir (string) csv file directory
        :param: preprocess_fn (function) function that transforms provided pd.DataFrame, should return pd.DataFrame
        :param: state_cols (list) list of column names (in pre-processed dataset) that represent current state
        :param: next_state_cols (list) list of column names (in pre-processed dataset) that represent next state
        :param: reward_cols (list) list of column names (in pre-processed dataset) that represent reward
        :param: info_cols (list) list of column names (in pre-processed dataset) that represent misc info
        :param: done_cols (list) list of column names (in pre-processed dataset) that flag if trip is last for driver
        :param: random_seed (int) seed to initialize pytorch, used in shuffle
    """

    def __init__(self, csv_file,
                 root_dir,
                 columns=None,
                 preprocess_fn=None,
                 state_cols=[],
                 next_state_cols=[],
                 reward_cols=[],
                 info_cols=[],
                 done_cols=[]):

        super(CSVDataset, self).__init__()

        try:
            # read csv file to pandas.DataFrame and optionally preprocess it
            self.data = pd.read_csv(os.path.join(root_dir, csv_file), names=columns)
            if preprocess_fn is not None:
                self.data = preprocess_fn(self.data)

            # find indexes of column names for state, next_state, reward, info, done
            cols = list(self.data.columns)
            self.state_cols_idx = [cols.index(c) for c in state_cols]
            self.next_state_cols_idx = [cols.index(c) for c in next_state_cols]
            self.reward_cols_idx = [cols.index(c) for c in reward_cols]
            self.info_cols_idx = [cols.index(c) for c in info_cols]
            self.done_cols_idx = [cols.index(c) for c in done_cols]

        except (IOError, FileNotFoundError) as e:
            raise RuntimeError('Problem with loading csv-file.\nDetails: %s' % e)
        except IndexError as e:
            raise IndexError('Invalid column names.\nDetails: %s' % e)

    def __getitem__(self, idx):
        """
        Get dataset item based on its index
            :param: idx (int): index of element
        """

        try:
            item = self.data.iloc[idx].to_numpy()
        except IndexError:
            raise IndexError('Index %d is out of the dataset bounds' % idx)

        # return state, reward, next_state, info, done

        state = {'cat': torch.LongTensor(item[self.state_cols_idx][:4].astype(np.int64)).to('cuda'), 'cont': torch.FloatTensor(item[self.state_cols_idx][4:].astype(np.float32)).to('cuda')}
        next_state = {'cat': torch.LongTensor(item[self.next_state_cols_idx][:4].astype(np.int64)).to('cuda'), 'cont': torch.FloatTensor(item[self.next_state_cols_idx][4:].astype(np.float32)).to('cuda')}

        return state, item[self.reward_cols_idx].astype(np.float32), next_state, item[self.info_cols_idx].astype(np.float32), item[self.done_cols_idx].astype(np.float32)

    def __len__(self):
        """
        Dataset length
        """
        return self.data.shape[0]

    @property
    def state_dim(self):
        return len(self.state_cols_idx)


def main():
    s = CSVDataset(csv_file='SAMPLE_didi_calc_rides_and_repositions.csv',
                   root_dir='../data',
                   state_cols=['pickup_weekday', 'pickup_hour', 'pickup_lon', 'pickup_lat'],
                   next_state_cols=['dropoff_weekday','dropoff_hour','dropoff_lon', 'dropoff_lat'],
                   reward_cols=['reward'],
                   info_cols=[],
                   done_cols=['done'])

    print(s.__getitem__(0))

    dl = torch.utils.data.DataLoader(s, batch_size=4, shuffle=True, drop_last=True)
    data_iter = iter(dl)
    print(data_iter.next())


if __name__ == '__main__':
    main()
