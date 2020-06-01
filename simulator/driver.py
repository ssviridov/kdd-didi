import itertools
import random
import numpy as np
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

    def generate_drivers(self, n_drivers: int):
        # logger.info("Start generating drivers")
        for _ in range(n_drivers):
            self.append(Driver(env=self.env))

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

    def get_dispatching_drivers(self):
        # logger.info("Start getting dispatching drivers")
        return self.get_drivers(status='idle') + self.get_drivers(status='reposition')

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

    def __init__(self, env):
        # logger.info(f"Start initializing driver")
        self.env = env
        self.driver_id = next(self.newid)
        self.driver_hex, self.driver_location = self.env.map.sample_driver_location()
        self.driver_reward = 0
        self.status = 'idle'
        self.order = None
        self.route = {self.env.t: self.driver_location}
        self.idle_time = abs(int(np.random.normal(300, 200)))

    def take_order(self, order_object, reward: float, pick_up_eta: float,
                   order_finish_timestamp: int, order_driver_distance: float):
        # logger.info(f"Start taking order {order_object.order_id} by driver {self.driver_id}")
        if self.status == 'assigned':
            raise DriverException(f'Driver {self.driver_id} already assigned to order {self.order.order_id}')
        elif self.status == 'not available':
            raise DriverException(f'Driver {self.driver_id} is not available at the moment')
        else:
            order_object.assigning(vehicle=self, reward=reward, pick_up_eta=pick_up_eta,
                                   order_finish_timestamp=order_finish_timestamp,
                                   order_driver_distance=order_driver_distance)
            self.order = order_object
            self.status = 'assigned'
            self.idle_time = 0

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
                self.env.orders_collection.cancel_orders([self.order])

    def _move(self):
        next_location = self.route.get(self.env.t)
        if not next_location:
            pass
        else:
            self.driver_hex = next_location
            self.driver_location = self.env.map.get_lonlat(next_location)
            del self.route[self.env.t]

    def cancel_order(self):
        # logger.info(f"Start cancelling order assigned to driver {self.driver_id}")
        if self.status == 'assigned':
            self.order = None
            self.status = 'idle'
