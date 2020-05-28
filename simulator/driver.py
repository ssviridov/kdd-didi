import itertools
import random
import numpy as np


class DriverException(Exception):
    pass


class DriversCollectionException(Exception):
    pass


class DriversCollection(list):
    randomizer = random.Random()

    def __init__(self, env):
        self.env = env
        super().__init__()

    def generate_drivers(self, n_drivers: int):
        for _ in range(n_drivers):
            self.append(Driver(env=self.env))

    def move_drivers(self):
        for driver in self:
            driver.move()

    def get_drivers(self, status: str, n_drivers=None):
        if status not in Driver.status_list:
            raise DriversCollectionException(f'status must be one of {Driver.status_list}')
        drivers = [i for i in self if i.status == status]
        if n_drivers and isinstance(n_drivers, int):
            self.randomizer.shuffle(drivers)
            return drivers[:n_drivers]
        else:
            return drivers

    def get_reposition_drivers(self, n_drivers: int):
        idle_drivers = self.get_drivers(status='idle')
        for_reposition = [i for i in idle_drivers if i.idle_time >= self.env.VALID_REPOSITION_TIME]
        return for_reposition[:n_drivers]

    def reposition(self, agent_response: list):
        for resp in agent_response:
            driver = self.get_by_driver_id(resp['driver_id'])
            driver.route = self.env.map.calculate_path(driver.driver_location, resp['destination'])
            driver.status = 'reposition'
            driver.idle_time = 0

    def idle_movement(self, model_response: list):
        for resp in model_response:
            driver = self.get_by_driver_id(resp['driver_id'])
            driver.route = self.env.map.calculate_path(driver.driver_location, resp['idle_hex'])

    def get_dispatching_drivers(self):
        return self.get_drivers(status='idle') + self.get_drivers(status='reposition')

    def get_by_driver_id(self, driver_id):
        drivers = {i.driver_id: i for i in self}
        if driver_id not in drivers.keys():
            raise DriversCollectionException(f'Driver_id={driver_id} does not exists')
        else:
            return drivers[driver_id]


class Driver:
    newid = itertools.count()
    status_list = ['idle', 'assigned', 'reposition']

    def __init__(self, env):
        self.env = env
        self.driver_id = next(self.newid)
        self.driver_location = self.env.map.sample_driver_location()
        self.driver_reward = 0
        self.status = 'idle'
        self.order = None
        self.route = {self.env.t: self.driver_location}
        self.idle_time = abs(int(np.random.normal(300, 200)))

    def take_order(self, order_object, reward_units: float, pick_up_eta: float,
                   order_finish_timestamp: int, order_driver_distance: float):
        if self.status == 'assigned':
            raise DriverException(f'Driver {self.driver_id} already assigned to order {self.order.order_id}')
        elif self.status == 'not available':
            raise DriverException(f'Driver {self.driver_id} is not available at the moment')
        else:
            order_object.assigning(vehicle=self, reward_units=reward_units, pick_up_eta=pick_up_eta,
                                   order_finish_timestamp=order_finish_timestamp,
                                   order_driver_distance=order_driver_distance)
            self.order = order_object
            self.status = 'assigned'
            self.idle_time = 0

    def move(self):
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
                self.driver_location = self.order.finish_location
                self.env.orders_collection.cancel_orders([self.order])

    def _move(self):
        next_location = self.route.get(self.env.t)
        if not next_location:
            pass
        else:
            self.driver_location = next_location
            del self.route[self.env.current_seconds]

    def cancel_order(self):
        if self.status == 'assigned':
            self.order = None
            self.status = 'idle'

