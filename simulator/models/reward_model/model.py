import pickle
import os
from joblib import load
import pandas as pd

from .utils import generate_dummy, convert_to_datetime, get_distance

import logging

logger = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(__file__))


class RewardModelException(Exception):
    pass


class RewardModel:
    def __init__(self):
        logger.info("Initialize reward model")
        with open(os.path.join(cur_dir, 'src', 'model.pickle'), 'rb') as f:
            self.model = pickle.load(f)
        with open(os.path.join(cur_dir, 'src', 'features.pickle'), 'rb') as f:
            self.features = pickle.load(f)

        self.request_keys = {'order_driver_distance',
                             'order_start_location',
                             'order_finish_location',
                             'driver_location',
                             'timestamp',
                             'order_finish_timestamp',
                             'day_of_week',
                             'pick_up_eta'}

    def predict(self, request):
        prepared_request = self._prepare_request(request)
        return self.model.predict(prepared_request[self.features].values.reshape(1, -1))[0][0]

    def partial_fit(self, X):
        raise NotImplemented

    def _prepare_request(self, request):
        if isinstance(request, dict):
            request = pd.Series(request)
        elif isinstance(request, pd.Series):
            pass
        else:
            raise RewardModelException('request must be one of dict or pd.Series types')

        if not self.request_keys.issubset(set(request.keys())):
            missed_keys = self.request_keys.difference(set(request.keys()))
            raise RewardModelException(f'{missed_keys} are missed in request')

        order_finish_dt = convert_to_datetime(request['order_finish_timestamp'])
        current_dt = convert_to_datetime(request['timestamp'])

        features = pd.Series()
        features['distance'] = get_distance(request['order_start_location'], request['order_finish_location'])
        features['duration'] = ((order_finish_dt - current_dt).total_seconds() - request['pick_up_eta']) / 60
        features = features.append(generate_dummy(request['day_of_week']+1, [i for i in range(1, 8)], 'p_w'))
        features = features.append(generate_dummy(current_dt.hour, [i for i in range(0, 24)], 'p_h'))
        features = features.append(generate_dummy(order_finish_dt.isoweekday(), [i for i in range(1, 8)], 'd_w'))
        features = features.append(generate_dummy(order_finish_dt.hour, [i for i in range(0, 24)], 'd_h'))

        return features

