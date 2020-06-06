import numpy as np
import logging

logger = logging.getLogger(__name__)


class DataCollector:
    def __init__(self, env, db_client=None):
        self.env = env
        self.db_client = db_client
        self.data = list()

        self._step_data = dict()
        self.init_step_data()

        if not self.db_client:
            self.data.append(self._step_data)
        else:
            self.db_client.write_simulation_step(self._step_data)
            self._step_data = dict()

    def init_step_data(self):
        self._step_data['step'] = self.env.t
        self._step_data['day_of_week'] = self.env.day_of_week
        self._step_data['total'] = dict(total_drivers=len(self.env.drivers_collection),
                                        total_orders=len(self.env.orders_collection),
                                        total_idle_drivers=len(self.env.drivers_collection.get_drivers(status='idle')),
                                        total_assigned=len(self.env.drivers_collection.get_drivers(status='assigned')),
                                        income_orders=0,
                                        income_drivers=0,
                                        outcome_drivers=0,
                                        assigned_orders=0,
                                        cancelled_orders=0,
                                        reward_earned=0,
                                        reward_cancelled=0)
        self._step_data['dispatching'] = dict(request=list(),
                                              assigned=list(),
                                              cancelled=list())
        self._step_data['repositioning'] = list()
        self._step_data['trajectories'] = list()

    def write_simulation_step(self):
        logger.info("Write simulation step")
        self._step_data['total']['total_drivers'] = len(self.env.drivers_collection)
        self._step_data['total']['total_orders'] = len(self.env.orders_collection)
        self._step_data['total']['total_idle_drivers'] = len(self.env.drivers_collection.get_drivers(status='idle'))
        self._step_data['total']['total_assigned'] = len(self.env.drivers_collection.get_drivers(status='assigned'))
        if not self.db_client:
            self.data.append(self._step_data)
        else:
            self.db_client.write_simulation_step(self._step_data)

    def collect_metric(self, key: str, value):
        self._step_data['total'][key] = value

    def collect_dispatching(self, request: list, assigned: list):
        # self._step_data['dispatching']['request'] = request
        # self._step_data['dispatching']['assigned'] = assigned
        assigned_ids = [i['order_id'] for i in assigned]
        self._step_data['total']['reward_earned'] = np.sum([i.reward for i in self.env.orders_collection
                                                            if i.order_id in assigned_ids])
        self._step_data['total']['assigned_orders'] = len(assigned_ids)

    def collect_cancelled(self, cancelled_list: list):
        # self._step_data['dispatching']['cancelled'] = [i.order_id for i in cancelled_list]
        self._step_data['total']['cancelled_orders'] = len(cancelled_list)
        self._step_data['total']['reward_cancelled'] = np.sum([i.reward for i in cancelled_list])
        self._step_data['total']['reward_earned'] -= self._step_data['total']['reward_cancelled']

    def collect_repositioning(self, repositioning_list):
        self._step_data['repositioning'] = repositioning_list

    def collect_trajectory(self, driver):
        self._step_data['trajectories'].extend(driver.trajectory)
                                                # {'driver_id': driver.driver_id,
                                                # 'born': driver.born,
                                                # 'died': driver.deadline,
                                                # 'trajectory': driver.trajectory})
