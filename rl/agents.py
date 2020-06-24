import torch
from torch import nn
import numpy as np
import pandas as pd
import torch.optim as optim
from sklearn.neighbors import NearestCentroid

import os
import json
from datetime import datetime as dt
from .utils import time_to_sincos, match
import warnings
warnings.filterwarnings('ignore')

cur_dir = os.path.dirname(os.path.abspath(__file__))


class BaseAgent:

    def dispatch(self, dispatch_observ):
        """ Compute the assignment between drivers and passengers at each time step
           :param dispatch_observ: a list of dict, the key in the dict includes:
               order_id, int
               driver_id, int
               order_driver_distance, float
               order_start_location, a list as [lng, lat], float
               order_finish_location, a list as [lng, lat], float
               driver_location, a list as [lng, lat], float
               timestamp, int
               order_finish_timestamp, int
               day_of_week, int
               reward_units, float
               pick_up_eta, float
           :return: a list of dict, the key in the dict includes:
               order_id and driver_id, the pair indicating the assignment
           """
        pass

    def reposition(self, repo_observ: dict):
        """ Compute the reposition action for the given drivers
            :param repo_observ: a dict, the key in the dict includes:
                timestamp: int
                driver_info: a list of dict, the key in the dict includes:
                    driver_id: driver_id of the idle driver in the treatment group, int
                    grid_id: id of the grid the driver is located at, str
                day_of_week: int
            :return: a list of dict, the key in the dict includes:
                driver_id: corresponding to the driver_id in the od_list
                destination: id of the grid the driver is repositioned to, str
            """
        repo_action = []
        for driver in repo_observ['driver_info']:
            # the default reposition is to let drivers stay where they are
            repo_action.append({'driver_id': driver['driver_id'], 'destination': driver['grid_id']})
        return repo_action

    def train(self):
        pass

    def save(self, path):
        pass


class ValueAgent(BaseAgent):

    def __init__(self, value_net_class,
                 replay_buffer,
                 batch_size=32,
                 gamma=0.99,
                 device="cpu",
                 optimizer=optim.Adam,
                 lr=1e-3,
                 state_dim=6,
                 hidden_dim=256,
                 criterion=nn.MSELoss(),
                 update=10):
        self.device = device
        self.value_net = value_net_class(state_dim, hidden_dim).to(self.device)
        self.target_value_net = value_net_class(state_dim, hidden_dim).to(self.device)

        for target_param, param in zip(self.target_value_net.parameters(), self.value_net.parameters()):
            target_param.data.copy_(param.data)

        self.replay_buffer = replay_buffer
        self.batch_size = batch_size
        self.gamma = gamma
        self.optimizer = optimizer(self.value_net.parameters(), lr=lr)
        self.criterion = criterion
        self.update = update
        self.iter = 0

        self.hexes = pd.read_csv(os.path.join(cur_dir, 'data', 'hexes.csv'), sep=';')

    def dispatch(self, dispatch_observ):
        weighted_observation = self.estimate_dispatching_request(dispatch_observ)
        _, dispatch_action = match(weighted_observation, weight='weight')
        return dispatch_action

    def reposition(self, repo_observ: dict):
        hexes = self.hexes.copy()

        hour = dt.fromtimestamp(repo_observ['timestamp']).hour
        h_sin, h_cos = time_to_sincos(hour, value_type='hour')
        d_sin, d_cos = time_to_sincos(repo_observ['day_of_week'], value_type='day_of_week')
        hexes['hour_sin'], hexes['hour_cos'] = [h_sin, h_cos]
        hexes['day_of_week_sin'], hexes['day_of_week_cos'] = [d_sin, d_cos]
        raw_states = hexes[['hour_sin', 'hour_cos', 'day_of_week_sin', 'day_of_week_cos', 'lon', 'lat']].values

        states = torch.FloatTensor(raw_states).to(self.device)
        hexes['value'] = self.value_net(states).detach().numpy().flatten()
        reposition_spots = hexes.nlargest(len(repo_observ['driver_info']), columns='value')

        drivers = pd.DataFrame(repo_observ['driver_info'])
        drivers_states = pd.merge(drivers, hexes, how='left',
                                  left_on='grid_id', right_on='hex').drop('hex', axis=1).sort_values('value')

        return [{'driver_id': driver, 'destination': grid}
                for driver, grid in zip(drivers_states.driver_id.values, reposition_spots.hex.values)]

    def train(self):
        state, reward, next_state, info, done = self.replay_buffer.sample(self.batch_size)

        state = torch.FloatTensor(state).to(self.device)
        reward = torch.FloatTensor(reward).unsqueeze(1).to(self.device)
        next_state = torch.FloatTensor(next_state).to(self.device)
        k = torch.FloatTensor(info).unsqueeze(1).to(self.device)

        value = self.value_net(state)
        # target_value = (reward * ((self.gamma ** k) - 1)) / (k * (self.gamma - 1)) + \
        #                (self.gamma ** k) * self.target_value_net(next_state)
        target_value = reward + self.gamma * self.target_value_net(next_state)

        value_loss = self.criterion(value, target_value.detach())

        self.optimizer.zero_grad()
        value_loss.backward()
        self.optimizer.step()

        self.iter += 1

        if self.iter % self.update == 0:
            for target_param, param in zip(self.target_value_net.parameters(), self.value_net.parameters()):
                target_param.data.copy_(param.data)

        return value_loss.detach().item(), value.detach().mean().item(), value.detach().std().item()

    def save(self, path):
        torch.save(self.value_net, path)

    def estimate_dispatching_request(self, request: list):
        if len(request) == 0:
            return []
        request_df = pd.DataFrame(request)
        needed_keys = {'timestamp', 'day_of_week', 'order_finish_timestamp',
                       'driver_location', 'order_finish_location', 'reward_units'}
        assert not needed_keys.difference(set(request_df.columns))

        state_tensor = self._prepare_state(request_df.copy(), time_col='timestamp',
                                           day_of_week_col='day_of_week', location_col='driver_location')
        next_state_tensor = self._prepare_state(request_df.copy(), time_col='order_finish_timestamp',
                                                day_of_week_col='day_of_week', location_col='order_finish_location')

        weights_tensor = self.gamma * self.value_net(next_state_tensor).flatten() - self.value_net(
            state_tensor).flatten()
        request_df['weight'] = weights_tensor.detach().numpy()
        return request_df.to_dict(orient='records')

    def _prepare_state(self, data: pd.DataFrame, time_col: str, day_of_week_col: str, location_col: str):
        data[['lon', 'lat']] = pd.DataFrame(data[location_col].to_list(), columns=['lon', 'lat'])
        data['hour'] = (pd.to_datetime(data[time_col], unit='s') + pd.DateOffset(hours=3)).dt.hour
        data['hour_sin'], data['hour_cos'] = time_to_sincos(data['hour'], value_type='hour')
        data['day_of_week_sin'], data['day_of_week_cos'] = time_to_sincos(data[day_of_week_col],
                                                                          value_type='day_of_week')
        data_matrix = data[['hour_sin', 'hour_cos', 'day_of_week_sin', 'day_of_week_cos', 'lon', 'lat']].values
        data_tensor = torch.FloatTensor(data_matrix).to(self.device)
        return data_tensor


class ValueAgentDataset(ValueAgent):
    def __init__(self, value_net_class,
                 dataset,
                 batch_size=32,
                 gamma=0.99,
                 device="cpu",
                 optimizer=optim.Adam,
                 lr=1e-3,
                 hidden_dim=256,
                 criterion=nn.MSELoss(),
                 update=10,
                 **kwargs):
        state_dim = dataset.state_dim
        super().__init__(value_net_class, None, batch_size, gamma, device, optimizer, lr, state_dim, hidden_dim,
                         criterion, update)
        self.dataloader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, **kwargs)
        self.iter_data = iter(self.dataloader)

    def train(self):
        try:
            state, reward, next_state, info, done = self.iter_data.next()
        except StopIteration:
            raise StopIteration('Dataset iterator has reached its end, call reset_iter()')

        state = state.to(self.device)
        reward = reward.to(self.device)
        next_state = next_state.to(self.device)
        k = info.to(self.device)

        value = self.value_net(state)
        # target_value = (reward * ((self.gamma ** k) - 1)) / (k * (self.gamma - 1)) + \
        #                (self.gamma ** k) * self.target_value_net(next_state)
        target_value = reward + self.gamma * self.target_value_net(next_state)

        value_loss = self.criterion(value, target_value.detach())

        self.optimizer.zero_grad()
        value_loss.backward()
        self.optimizer.step()

        self.iter += 1

        if self.iter % self.update == 0:
            for target_param, param in zip(self.target_value_net.parameters(), self.value_net.parameters()):
                target_param.data.copy_(param.data)

        return value_loss.detach().item(), value.detach().mean().item(), value.detach().std().item()

    def reset_iter(self):
        self.iter_data = iter(self.dataloader)


class ValueRanksAgent(ValueAgent):

    STATE_REPRESENTATION = ['hour_sin', 'hour_cos', 'day_of_week_sin', 'day_of_week_cos',
                            'lon', 'lat', 'pickup_rank', 'dropoff_rank', 'minute', 'minute_5', 'minute_10']

    def __init__(self, **kwargs):
        with open(os.path.join(cur_dir, 'data', 'grids_ranks.json'), 'r') as f:
            self.grids_ranks = json.load(f)
        super().__init__(**kwargs)
        self.grid_model = NearestCentroid()
        self.grid_model.fit(self.hexes[['lon', 'lat']].values, self.hexes.index.values)

    def reposition(self, repo_observ: dict):
        hexes = self.hexes.copy()

        hour = dt.fromtimestamp(repo_observ['timestamp']).hour
        minute = dt.fromtimestamp(repo_observ['timestamp']).minute
        seconds = dt.fromtimestamp(repo_observ['timestamp']).second
        t = hour * 60 * 60 + minute * 60 + seconds

        h_sin, h_cos = time_to_sincos(hour, value_type='hour')
        d_sin, d_cos = time_to_sincos(repo_observ['day_of_week'], value_type='day_of_week')
        hexes['hour_sin'], hexes['hour_cos'] = [h_sin, h_cos]
        hexes['day_of_week_sin'], hexes['day_of_week_cos'] = [d_sin, d_cos]
        hexes['idx'] = hexes['hex'] + '_' + str(repo_observ['day_of_week']) + '_' + str(hour)
        ranks = pd.DataFrame([self.grids_ranks.get(idx, {'pickup_rank': 0, 'dropoff_rank': 0}) for idx in hexes['idx']])
        hexes[['pickup_rank', 'dropoff_rank']] = ranks
        hexes['minute'] = int(t / 60)
        hexes['minute_5'] = int(t / 60 / 5)
        hexes['minute_10'] = int(t / 60 / 10)

        raw_states = hexes[self.STATE_REPRESENTATION].values

        states = torch.FloatTensor(raw_states).to(self.device)
        hexes['value'] = self.value_net(states).detach().numpy().flatten()
        reposition_spots = hexes.nlargest(len(repo_observ['driver_info']), columns='value')

        drivers = pd.DataFrame(repo_observ['driver_info'])
        drivers_states = pd.merge(drivers, hexes, how='left',
                                  left_on='grid_id', right_on='hex').drop('hex', axis=1).sort_values('value')

        return [{'driver_id': driver, 'destination': grid}
                for driver, grid in zip(drivers_states.driver_id.values, reposition_spots.hex.values)]

    def _get_hex_by_lonalat(self, lonlat_df: pd.DataFrame):
        lonlat_df.loc[:, 'centroid'] = self.grid_model.predict(lonlat_df[['lon', 'lat']].values)
        return pd.merge(lonlat_df, self.hexes, how='left', left_on='centroid', right_index=True).hex

    def _get_grid_ranks(self, df):
        data = df.copy()
        data['hex_id'] = self._get_hex_by_lonalat(data[['lon', 'lat']])
        data['idx'] = data['hex_id'] + '_' + data['day_of_week'].astype(str) + '_' + data['hour'].astype(str)
        ranks = pd.DataFrame([self.grids_ranks.get(idx, {'pickup_rank': 0, 'dropoff_rank': 0}) for idx in data['idx']])
        return ranks['pickup_rank'], ranks['dropoff_rank']

    def _prepare_state(self, data: pd.DataFrame, time_col: str, day_of_week_col: str, location_col: str):
        data[['lon', 'lat']] = pd.DataFrame(data[location_col].to_list(), columns=['lon', 'lat'])
        data['hour'] = (pd.to_datetime(data[time_col], unit='s') + pd.DateOffset(hours=3)).dt.hour
        data['minute'] = (pd.to_datetime(data[time_col], unit='s') + pd.DateOffset(hours=3)).dt.minute
        data['seconds'] = (pd.to_datetime(data[time_col], unit='s') + pd.DateOffset(hours=3)).dt.second
        data['t'] = data['hour'] * 60 * 60 + data['minute'] * 60 + data['seconds']
        data['hour_sin'], data['hour_cos'] = time_to_sincos(data['hour'], value_type='hour')
        data['day_of_week_sin'], data['day_of_week_cos'] = time_to_sincos(data[day_of_week_col],
                                                                          value_type='day_of_week')
        data['minute'] = (data['t'] / 60).astype(int)
        data['minute_5'] = (data['t'] / 60 / 5).astype(int)
        data['minute_10'] = (data['t'] / 60 / 10).astype(int)
        data['pickup_rank'], data['dropoff_rank'] = self._get_grid_ranks(data)

        data_matrix = data[self.STATE_REPRESENTATION].values
        data_tensor = torch.FloatTensor(data_matrix).to(self.device)
        return data_tensor

# if __name__ == '__main__':
#     from buffers import MongoDBReplayBuffer
#     from models import ValueNetwork
#
#     buffer = MongoDBReplayBuffer()
#     agent = ValueAgent(replay_buffer=buffer, value_net_class=ValueNetwork)
#
#     losses = list()
#     for i in range(1000):
#         loss = agent.train()
#         losses.append(loss)
#         print(i, loss)
#
#     test_request = [{'reward_units': 2.2887375583652845, 'order_id': 3, 'driver_id': 0,
#                      'order_start_location': [104.04430663835227, 30.681486716584075],
#                      'order_finish_location': [104.0441221042292, 30.63796116557718],
#                      'driver_location': [104.04190794375882, 30.683316711804967], 'timestamp': 1591563608,
#                      'day_of_week': 1, 'order_driver_distance': 306.573911706953, 'pick_up_eta': 38.32173896336913,
#                      'distance': 5.533611851509747, 'order_finish_timestamp': 1591564337},
#                     {'reward_units': 2.5521145410223762, 'order_id': 5, 'driver_id': 0,
#                      'order_start_location': [104.04178862062531, 30.674484810171272],
#                      'order_finish_location': [104.10461729030528, 30.680566290308228],
#                      'driver_location': [104.04190794375882, 30.683316711804967], 'timestamp': 1591563608,
#                      'day_of_week': 1, 'order_driver_distance': 979.2063672052939, 'pick_up_eta': 122.40079590066173,
#                      'distance': 6.229383663898964, 'order_finish_timestamp': 1591564508}]
#
#     action = agent.dispatch(test_request)
#     print(action)
