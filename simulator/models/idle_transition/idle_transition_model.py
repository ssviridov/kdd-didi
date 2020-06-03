import os
import pickle
import random

cur_dir = os.path.dirname(os.path.abspath(__file__))
data_file = os.path.join(cur_dir, "data", "idle_trans_data.pickle")


class IdleTransitionModel:

    def __init__(self, data_file=data_file):
        try:
            with open(data_file, 'rb') as f:
                self.idle_trans_data = pickle.load(f)
        except pickle.PickleError:
            raise RuntimeError("Can't load data with idle transitions")

    def get_driver_idle_transition(self, driver_data):
        driver_transitions = []
        try:
            for driver in driver_data['idle_drivers']:
                driver_transitions.append({'driver_id': driver['driver_id'],
                                           'idle_hex': self._get_transition(driver['driver_location'],
                                                                            driver_data['hour'])})
        except KeyError:
            raise KeyError('Wrong data structure for driver_data')

        return driver_transitions

    def _get_transition(self, hex_id, hour):
        key = hex_id + '_' + str(int(hour))
        try:
            idle_trans = self.idle_trans_data[key]
            dest_hex_id = idle_trans['h']
            self_transition_prob = idle_trans['sp']

            # with self_trans_prob driver is laying on the same hex
            if random.random() <= self_transition_prob:
                return hex_id
            # else - move to dest_hex
            return dest_hex_id

        # no such hex-hour pair in data --> laying on the same hex
        # REALLY exceptional case
        except KeyError:
            return hex_id


if __name__ == '__main__':
    itModel = IdleTransitionModel()
