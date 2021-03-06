import datetime as dt
import itertools
import numpy as np

from .driver import DriversCollection
from .order import OrdersCollection
from .map import Map
from .utils import DataCollector, prepare_dispatching_request, handle_dispatching_response
from .models.order_generator import OrderGenerator
from .models.driver_generator import DriverGenerator
from .models.cancel_model import CancelModel
from .models.idle_transition import IdleTransitionModel

import logging

logger = logging.getLogger(__name__)


class Environment:
    VALID_REPOSITION_TIME = 300
    MAX_PICKUP_DISTANCE = 2000
    IDLE_SPEED_M_PER_S = 5
    REPO_SPEED_M_PER_S = 3
    PICKUP_SPEED_M_PER_S = 8
    STEP_UNIT = 1

    def __init__(self, day_of_week: int, agent, db_client, random_seed=None):
        logger.info("Create environment")
        self.day_of_week = day_of_week
        self.t = 0
        self.hours = 0
        self.seconds = 0
        self.minutes = 0
        self.start_timestamp = self.timestamp

        self.drivers_collection = DriversCollection(env=self)
        self.orders_collection = OrdersCollection(env=self)
        self.map = Map(env=self, random_seed=random_seed)

        self.agent = agent
        self.total_reward = 0

        self.d_orders = None
        self.d_drivers = None

        self.cancel_model = CancelModel(weekday=day_of_week, random_seed=random_seed)

        self.datacollector = DataCollector(env=self, db_client=db_client)
        self.idle_trans_model = IdleTransitionModel(random_seed=random_seed)

        self.random_seed = random_seed
        if random_seed:
            np.random.seed(random_seed)

    def update_current_time(self, current_seconds):
        logger.info(f"[{current_seconds}s] simulation time")
        self.t = current_seconds
        self.hours = current_seconds // (60 * 60)
        self.minutes = (current_seconds - self.hours * 60 * 60) // 60
        self.seconds = current_seconds - self.hours * 60 * 60 - self.minutes * 60
        self.datacollector.init_step_data()

    @property
    def timestamp(self):
        if self.hours == 24:
            return int(dt.datetime.combine(dt.date.today() + dt.timedelta(days=1),
                                           dt.time(0, self.minutes, self.seconds)).timestamp())
        else:
            return int(dt.datetime.combine(dt.date.today(),
                                           dt.time(self.hours, self.minutes, self.seconds)).timestamp())

    def reposition_actions(self):
        logger.debug("Start reposition action")
        all_idle_drivers = self.drivers_collection.get_drivers('idle')

        # Valid for Repositioning & Agent Repositioning Selection models
        repositioning_drivers = self.drivers_collection.get_reposition_drivers(n_drivers=5)
        idle_drivers = [i for i in all_idle_drivers if i not in repositioning_drivers and not i.route]

        # Driver repositioning Model
        if len(repositioning_drivers) > 0:
            self._repositioning(repositioning_drivers)

    def idle_movement(self):
        logger.debug("Idle movement")
        all_idle_drivers = self.drivers_collection.get_drivers('idle')
        idle_drivers = [i for i in all_idle_drivers if not i.route]
        self._idle_movement(idle_drivers)

    def generate_orders(self):
        logger.debug("Start generating orders for day")
        order_gen = OrderGenerator(random_seed=self.random_seed)
        self.d_orders = order_gen.generate_orders(weekday=self.day_of_week)

    def generate_drivers(self):
        logger.debug("Start generating drivers for day")
        driver_gen = DriverGenerator(random_seed=self.random_seed)
        self.d_drivers = driver_gen.generate_drivers(weekday=self.day_of_week)

    def get_orders_for_second(self):
        logger.debug("Get orders for this simulation second")
        orders = self.d_orders.get(self.t, [])
        self.datacollector._step_data['total']['income_orders'] = len(orders)
        self.orders_collection.add_orders(orders)

    def balancing_drivers(self):
        logger.debug("Start making drivers online/offline")
        # generating new drivers
        drivers = self.d_drivers.get(self.t, [])
        self.datacollector._step_data['total']['income_drivers'] = len(drivers)
        self.drivers_collection.add_drivers(drivers)
        # deleting old drivers
        deleted_amt = self.drivers_collection.delete_drivers()
        self.datacollector._step_data['total']['outcome_drivers'] = deleted_amt

    def dispatching_actions(self):
        logger.debug("Start dispatch action")
        all_idle_drivers = self.drivers_collection.get_dispatching_drivers()
        orders = self.orders_collection.get_orders(status="unassigned")

        # All idle drivers all eligible for dispatching
        # Order-Driver Matching Model
        assigned_orders = self._dispatching(orders=orders, drivers=all_idle_drivers)

        self.cancel_orders(assigned_orders)

    def cancel_orders(self, assigned_orders: list):
        logger.debug("Start cancelling orders")
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
        logger.debug("Start moving drivers")
        self.drivers_collection.move_drivers()

    def _repositioning(self, drivers_for_reposition: list):
        prepared_request = dict(driver_info=[{'driver_id': d.driver_id,
                                              'grid_id': d.driver_hex} for d in drivers_for_reposition],
                                day_of_week=self.day_of_week,
                                timestamp=self.timestamp)
        agent_response = self.agent.reposition(prepared_request)
        self.drivers_collection.reposition(agent_response)

    def _idle_movement(self, idle_drivers: list):
        prepared_request = dict(idle_drivers=[{'driver_id': d.driver_id,
                                               'driver_location': d.driver_hex} for d in idle_drivers],
                                day_of_week=self.day_of_week,
                                hour=self.hours)
        model_response = self.idle_trans_model.get_driver_idle_transition(prepared_request)
        self.drivers_collection.idle_movement(model_response)

    def _dispatching(self, orders, drivers):
        prepared_request = prepare_dispatching_request(env=self, drivers=drivers, orders=orders)
        logger.debug("Start agent dispatch")
        agent_response = self.agent.dispatch(dispatch_observ=prepared_request)
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
