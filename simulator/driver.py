import itertools
import random
import numpy as np
from datetime import datetime as dt
import hashlib
import logging

logger = logging.getLogger(__name__)


class DriverException(Exception):
    pass


class DriversCollectionException(Exception):
    pass


class DriversCollection(list):
    randomizer = random.Random()

    def __init__(self, env):
        logger.info(f"Start initializing driver collection")
        self.env = env
        super().__init__()

    def delete_drivers(self):
        # logger.info("Start deleting drivers")
        deleted_drivers = 0
        for driver in self:
            if (driver.status != 'assigned') and (driver.deadline <= self.env.t):
                driver.update_trajectory('idle', terminal_state=True)
                self.env.datacollector.collect_trajectory(driver)
                self.remove(driver)
                deleted_drivers += 1
        return deleted_drivers

    def add_drivers(self, drivers: list):
        # logger.info("Start adding drivers")
        for start_hex, lifetime in drivers:
            self.append(Driver(env=self.env, start_hex=start_hex, lifetime=lifetime))

    def move_drivers(self):
        # logger.info("Start moving drivers")
        for driver in self:
            driver.move()

    def get_drivers(self, status: str, n_drivers=None):
        # logger.info("Start getting drivers")
        if status not in Driver.status_list:
            raise DriversCollectionException(f'status must be one of {Driver.status_list}')
        drivers = [i for i in self if i.status == status]
        if n_drivers and isinstance(n_drivers, int):
            self.randomizer.shuffle(drivers)
            return drivers[:n_drivers]
        else:
            return drivers

    def get_reposition_drivers(self, n_drivers: int):
        # logger.info("Start getting drivers for reposition")
        idle_drivers = self.get_drivers(status='idle')
        for_reposition = [i for i in idle_drivers if i.idle_time >= self.env.VALID_REPOSITION_TIME]
        return for_reposition[:n_drivers]

    def reposition(self, agent_response: list):
        # logger.info("Start repositioning drivers")
        repositioning_data = list()
        for resp in agent_response:
            driver = self.get_by_driver_id(resp['driver_id'])
            driver.route = self.env.map.calculate_path(driver.driver_hex, resp['destination'])
            driver.status = 'reposition'
            driver.idle_time = 0
            resp['driver_hex'] = driver.driver_hex
            repositioning_data.append(resp)
        self.env.datacollector.collect_repositioning(repositioning_data)

    def idle_movement(self, model_response: list):
        # logger.info("Start moving idle drivers")
        for resp in model_response:
            driver = self.get_by_driver_id(resp['driver_id'])
            driver.route = self.env.map.calculate_path(driver.driver_hex, resp['idle_hex'])
            driver.update_trajectory('idle')

    def get_dispatching_drivers(self):
        # logger.info("Start getting dispatching drivers")
        return [i for i in self if i.status != 'assigned']

    def get_by_driver_id(self, driver_id):
        # logger.info("Start get driver by id")
        drivers = {i.driver_id: i for i in self}
        if driver_id not in drivers.keys():
            raise DriversCollectionException(f'Driver_id={driver_id} does not exists')
        else:
            return drivers[driver_id]


class Driver:
    newid = itertools.count()
    status_list = ['idle', 'assigned', 'reposition']

    def __init__(self, env, start_hex, lifetime=None):
        # logger.info(f"Start initializing driver")
        self.env = env
        self.driver_id = next(self.newid)
        self.driver_hex = start_hex
        self.driver_location = self.env.map.get_lonlat(start_hex)
        self.driver_reward = 0
        self.born = self.env.t
        self.deadline = lifetime + self.env.t
        self.status = 'idle'
        self.order = None
        self.route = {}
        self.idle_time = abs(int(np.random.normal(300, 200)))
        self._sample_start = None
        self._sample_end = None
        self.trajectory = []
        self._trajectory_id = self._generate_trajectory_id()

    def take_order(self, order_object, reward: float, pick_up_eta: float,
                   order_finish_timestamp: int, order_driver_distance: float):
        # logger.info(f"Start taking order {order_object.order_id} by driver {self.driver_id}")
        if self.status == 'assigned':
            raise DriverException(f'Driver {self.driver_id} already assigned to order {self.order.order_id}')
        else:
            order_object.assigning(vehicle=self, reward=reward, pick_up_eta=pick_up_eta,
                                   order_finish_timestamp=order_finish_timestamp,
                                   order_driver_distance=order_driver_distance)
            self.order = order_object
            self.status = 'assigned'
            self.idle_time = 0
            self.update_trajectory('assigned')

    def move(self):
        # logger.info(f"Start moving driver {self.driver_id}")
        if self.status != 'assigned':
            self._move()
            if self.status == 'idle':
                self.idle_time += self.env.STEP_UNIT
            if self.status == 'reposition' and not self.route:
                self.status = 'idle'
        else:
            if self.env.timestamp == self.order.order_finish_timestamp:
                self.driver_reward += self.order.reward
                self.env.total_reward += self.order.reward
                self.driver_location = self.order.order_finish_location
                self.driver_hex = self.order.finish_hex
                self.env.orders_collection.cancel_orders([self.order], finish=True)

    def _move(self):
        next_location = self.route.get(self.env.t)
        if not next_location:
            pass
        else:
            self.driver_hex = next_location
            self.driver_location = self.env.map.get_lonlat(next_location)
            del self.route[self.env.t]

    def update_trajectory(self, task_type: str, terminal_state=False):
        done = int(terminal_state)
        if task_type == 'idle':
            # if first sample or sample after order completion
            if not self._sample_start:
                self._sample_start = dict(traj_id_start=self._trajectory_id, t_start=self.env.t,
                                          day_of_week_start=self.env.day_of_week, status_start=self.status,
                                          hex_start=self.driver_hex, loc_start=self.driver_location,
                                          route_length_start=self.route, reward_start=0, done_start=done)
                # if first point is terminal (e.g. short life or death after order completion)
                if terminal_state:
                    self._sample_end = {k.replace('start', 'end'): v for k, v in self._sample_start.items()}
                    if len(self.trajectory) == 0:
                        self.trajectory.append({**self._sample_start, **self._sample_end})
                    else:
                        last_sample_end = [i for i in self.trajectory
                                           if i['t_end'] == max([i['t_end'] for i in self.trajectory])][0]
                        self._sample_start = {k.replace('end', 'start'): v for k, v in last_sample_end.items()
                                              if 'start' not in k}
                        self.trajectory.append({**self._sample_start, **self._sample_end})
            # is idle movement is doing nothing - don't collect it
            elif self._sample_start['status_start'] == 'idle' and \
                    self._sample_start['hex_start'] == self.driver_hex and done == 0:
                return
            else:
                self._sample_end = dict(traj_id_end=self._trajectory_id, t_end=self.env.t,
                                        day_of_week_end=self.env.day_of_week, status_end=self.status,
                                        hex_end=self.driver_hex, loc_end=self.driver_location,
                                        route_length_end=self.route, reward_end=0, done_end=done)
                self.trajectory.append({**self._sample_start, **self._sample_end})
                self._sample_start = {key.replace('end', 'start'): value for key, value in self._sample_end.items()}

        if task_type == 'assigned':
            status = self.status + '_' + str(self.order.order_id)
            finish_dt = dt.fromtimestamp(self.order.order_finish_timestamp)
            finish_seconds = finish_dt.hour * 60 * 60 + finish_dt.minute * 60 + finish_dt.second

            # Collect assignment
            self._sample_end = dict(traj_id_end=self._trajectory_id, t_end=self.env.t,
                                    day_of_week_end=self.env.day_of_week, status_end=status, hex_end=self.driver_hex,
                                    loc_end=self.driver_location, reward_end=0, done_end=done)
            self.trajectory.append({**self._sample_start, **self._sample_end})

            # Collect order completion
            self._sample_start = {key.replace('end', 'start'): value for key, value in self._sample_end.items()}
            self._sample_end = dict(traj_id_end=self._trajectory_id, t_end=finish_seconds,
                                    day_of_week_end=self.env.day_of_week, status_end=status,
                                    hex_end=self.order.finish_hex, loc_end=self.order.order_finish_location,
                                    reward_end=self.order.reward, done_end=done)
            self.trajectory.append({**self._sample_start, **self._sample_end})
            self._sample_start = None

    def cancel_order(self, finish=False):
        # logger.info(f"Start cancelling order assigned to driver {self.driver_id}")
        if self.status == 'assigned':
            if not finish:
                traj_status = self.status + '_' + str(self.order.order_id)
                self.trajectory = [i for i in self.trajectory if
                                   not (i['status_start'] == traj_status and i['status_end'] == traj_status)]
                old_start = [i for i in self.trajectory if i['status_end'] == traj_status][0]
                self._sample_start = {k: v for k, v in old_start.items() if 'end' not in k}
                self.trajectory = [i for i in self.trajectory if i['status_end'] != traj_status]
            self.order = None
            self.status = 'idle'
            self.route = {}

    def _generate_trajectory_id(self):
        string = f'{str(self.driver_id)}_{str(self.driver_location)}'
        return hashlib.md5(string.encode('utf-8')).hexdigest()
