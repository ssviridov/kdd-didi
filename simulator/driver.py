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
        return for_reposition[n_drivers]

    def get_dispatching_drivers(self):
        return self.get_drivers(status='idle')


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
        self.next_idle_location = [self.driver_location]
        self.idle_time = abs(int(np.random.normal(300, 200)))

    @property
    def driver_grid(self):
        return self.env.map.get_grid(self.driver_location)

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
        if self.status == 'idle':
            self.driver_location = self.next_idle_location[0] #?
            del self.next_idle_location[0]
            self.idle_time += self.env.params.STEP_UNIT
        elif self.status == 'assigned':
            if self.env.timestamp == self.order.order_finish_timestamp:
                self.driver_reward += self.order.reward
                self.env.total_reward += self.order.reward
                self.status = 'idle'
                self.driver_location = self.order.finish_location
        else:
            pass

    def cancel_order(self):
        if self.status == 'assigned':
            self.order = None
            self.status = 'idle'

