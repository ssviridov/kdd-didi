import datetime as dt
import itertools
import random
import numpy as np

from driver import DriversCollection
from order import OrdersCollection
from agent import Agent
from map import Map
from utils import DataCollector, prepare_dispatching_request, handle_dispatching_response
from models.order_generator import OrderGenerator
from models.cancel_model import CancelModel

import logging

logger = logging.getLogger(__name__)


class Environment:
    VALID_REPOSITION_TIME = 300
    MAX_PICKUP_DISTANCE = 2000
    IDLE_SPEED_M_PER_S = 3
    REPO_SPEED_M_PER_S = 3
    PICKUP_SPEED_M_PER_S = 3
    STEP_UNIT = 1

    def __init__(self, day_of_week: int, agent: Agent, db_client):
        logger.info("Create environment")
        self.day_of_week = day_of_week
        self.t = 0
        self.hours = 0
        self.seconds = 0
        self.minutes = 0

        self.drivers_collection = DriversCollection(env=self)
        self.orders_collection = OrdersCollection(env=self)
        self.map = Map(env=self)

        self.agent = agent
        self.total_reward = 0

        # Init some drivers
        # Somehow calculate number of drivers for init
        self.drivers_collection.generate_drivers(n_drivers=10000)

        self.d_orders = None

        self.cancel_model = CancelModel(weekday=day_of_week)

        self.datacollector = DataCollector(env=self, db_client=db_client)

    def update_current_time(self, current_seconds):
        logger.info(f"[{current_seconds}s] - simulation time")
        self.t = current_seconds
        self.hours = current_seconds // (60 * 60)
        self.minutes = (current_seconds - self.hours * 60 * 60) // 60
        self.seconds = current_seconds - self.hours * 60 * 60 - self.minutes * 60
        self.datacollector.init_step_data()

    @property
    def timestamp(self):
        if self.hours == 24:
            return int(dt.datetime.combine(dt.date.today()+dt.timedelta(days=1),
                                           dt.time(0, self.minutes, self.seconds)).timestamp())
        else:
            return int(dt.datetime.combine(dt.date.today(),
                                           dt.time(self.hours, self.minutes, self.seconds)).timestamp())

    def reposition_actions(self):
        logger.info("Start reposition action")
        all_idle_drivers = self.drivers_collection.get_drivers('idle')

        # Valid for Repositioning & Agent Repositioning Selection models
        repositioning_drivers = self.drivers_collection.get_reposition_drivers(n_drivers=5)
        idle_drivers = [i for i in all_idle_drivers if i not in repositioning_drivers and not i.route]

        # Driver repositioning Model
        self._repositioning(repositioning_drivers)

        # Idle Drivers Movement Model (just assigning Driver.next_idle_location without move())
        self._idle_movement(idle_drivers)

    def generate_orders(self):
        logger.info("Start generating orders for day")
        order_gen = OrderGenerator()
        self.d_orders = order_gen.generate_orders(weekday=self.day_of_week)

    def get_orders_for_second(self):
        logger.info("Get orders for this simulation second")
        orders = self.d_orders.get(self.t, [])
        self.datacollector._step_data['total']['income_orders'] = len(orders)
        self.orders_collection.add_orders(orders)

    def balancing_drivers(self):
        logger.info("Start making drivers online/offline")
        # TODO balance number of drivers in system by deleting and generating new ones
        pass

    def dispatching_actions(self):
        logger.info("Start dispatch action")
        all_idle_drivers = self.drivers_collection.get_dispatching_drivers()
        orders = self.orders_collection.get_orders(status="unassigned")

        # All idle drivers all eligible for dispatching
        # Order-Driver Matching Model
        assigned_orders = self._dispatching(orders=orders, drivers=all_idle_drivers)

        self.cancel_orders(assigned_orders)

    def cancel_orders(self, assigned_orders: list):
        logger.info("Start cancelling orders")
        orders = self.orders_collection.get_orders(status="assigned")
        orders = [order for order in orders if order.order_id in [d['order_id'] for d in assigned_orders]]
        if not orders:
            return None
        all_probs = self.cancel_model.sample_probs(len(orders))
        idx = [int(order.order_driver_distance // 200) for order in orders]  # NOTE: 0 <= order_driver_distance < 2000
        order_probs = np.choose(idx, all_probs)
        orders_to_cancel = list(itertools.compress(orders, np.random.binomial(1, order_probs)))
        self.datacollector.collect_cancelled(orders_to_cancel)
        self.orders_collection.cancel_orders(orders_to_cancel)

    def move_drivers(self):
        logger.info("Start moving drivers")
        self.drivers_collection.move_drivers()

    def _repositioning(self, drivers_for_reposition: list):
        prepared_request = dict(driver_info=[{'driver_id': d.driver_id,
                                              'grid_id': d.driver_hex} for d in drivers_for_reposition],
                                day_of_week=self.day_of_week,
                                timestamp=self.timestamp)
        agent_response = self.agent.reposition(prepared_request)
        self.drivers_collection.reposition(agent_response)

    def _idle_movement(self, idle_drivers: list):
        prepared_request = dict(idle_driver=[{'driver_id': d.driver_id,
                                              'driver_location': d.driver_hex} for d in idle_drivers],
                                day_of_week=self.day_of_week,
                                hour=self.hours)
        # TODO Use idle transition probability model for assigning next_idle_location
        model_response = [{'driver_id': d.driver_id, 'idle_hex': d.driver_hex} for d in idle_drivers]
        self.drivers_collection.idle_movement(model_response)

    def _dispatching(self, orders, drivers):
        prepared_request = prepare_dispatching_request(env=self, drivers=drivers, orders=orders)
        logger.info("Send dispatching request to Agent")
        agent_response = self.agent.dispatch(prepared_request)
        handle_dispatching_response(env=self, agent_request=prepared_request, agent_response=agent_response)
        self.datacollector.collect_dispatching(prepared_request, agent_response)
        self.orders_collection.delete_unassigned_orders()
        return agent_response

# if __name__ == '__main__':
#     a = Agent()
#     env = Environment(3, a)
#     idle = env.drivers_collection.get_reposition_drivers(n_drivers=5)
#     env._repositioning(idle)
#     print('hello')
