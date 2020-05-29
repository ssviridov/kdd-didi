import datetime as dt
import itertools
import random
import numpy as np

from driver import DriversCollection
from order import OrdersCollection
from agent import Agent
from map import Map
from models.order_generator import OrderGenerator
from models.cancel_model import CancelModel


class Environment:
    VALID_REPOSITION_TIME = 300
    IDLE_SPEED_M_PER_S = 3
    REPO_SPEED_M_PER_S = 3
    STEP_UNIT = 1

    def __init__(self, day_of_week: int, agent: Agent):
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
        self.drivers_collection.generate_drivers(n_drivers=100)

        self.df_orders = None

        self.cancel_model = CancelModel()

    def update_current_time(self, current_seconds):
        self.t = current_seconds
        self.hours = current_seconds // (60 * 60)
        self.minutes = (current_seconds - self.hours * 60 * 60) // 60
        self.seconds = current_seconds - self.hours * 60 * 60 - self.minutes * 60

    @property
    def timestamp(self):
        return int(dt.datetime.combine(dt.date.today(), dt.time(self.hours, self.minutes, self.seconds)).timestamp())

    def reposition_actions(self):
        all_idle_drivers = self.drivers_collection.get_drivers('idle')

        # Valid for Repositioning & Agent Repositioning Selection models
        repositioning_drivers = self.drivers_collection.get_reposition_drivers(n_drivers=5)
        idle_drivers = [i for i in all_idle_drivers if i not in repositioning_drivers and not i.route]

        # Driver repositioning Model
        self._repositioning(repositioning_drivers)

        # Idle Drivers Movement Model (just assigning Driver.next_idle_location without move())
        self._idle_movement(idle_drivers)

    def generate_orders(self):
        order_gen = OrderGenerator()
        self.df_orders = order_gen.generate_orders(weekday=self.day_of_week)

    def get_orders_for_second(self):
        orders = self.df_orders.loc[
            self.df_orders["order_time"] == dt.timedelta(hours=self.hours, minutes=self.minutes, seconds=self.seconds),
            ["pickup_hex", "dropoff_hex"]]
        self.orders_collection.add_orders(orders)

    def balancing_drivers(self):
        # TODO balance number of drivers in system by deleting and generating new ones
        pass

    def dispatching_actions(self):
        all_idle_drivers = self.drivers_collection.get_dispatching_drivers()
        orders = self.orders_collection.get_orders(status="unassigned")

        # All idle drivers all eligible for dispatching
        # Order-Driver Matching Model
        self._dispatching(orders=orders, drivers=all_idle_drivers)

    def cancel_orders(self):
        orders = self.orders_collection.get_orders(status="assigned")
        if not orders:
            return None
        all_probs = self.cancel_model.sample_probs(len(orders))
        idx = [order.order_driver_distance // 200 for order in orders]
        order_probs = np.choose(idx, all_probs)
        orders_to_cancel = list(itertools.compress(orders, np.random.binomial(1, order_probs)))
        self.orders_collection.cancel_orders(orders_to_cancel)

    def move_drivers(self):
        self.drivers_collection.move_drivers()

    def _repositioning(self, drivers_for_reposition: list):
        prepared_request = dict(driver_info=[{'driver_id': d.driver_id,
                                              'grid_id': d.driver_location} for d in drivers_for_reposition],
                                day_of_week=self.day_of_week,
                                timestamp=self.timestamp)
        agent_response = self.agent.reposition(prepared_request)
        self.drivers_collection.reposition(agent_response)

    def _idle_movement(self, idle_drivers: list):
        prepared_request = dict(idle_driver=[{'driver_id': d.driver_id,
                                              'driver_location': d.driver_location} for d in idle_drivers],
                                day_of_week=self.day_of_week,
                                hour=self.hours)
        # TODO Use idle transition probability model for assigning next_idle_location
        model_response = [{'driver_id': d.driver_id, 'idle_hex': d.driver_location} for d in idle_drivers]
        self.drivers_collection.idle_movement(model_response)

    def _dispatching(self, orders, drivers):
        # TODO: prepare request for agent.dispatch()
        prepared_request = [
            {"order_id": order.order_id, "driver_id": driver.driver_id, "reward_units": random.random()}
            for order, driver in itertools.product(orders, drivers)]
        agent_response = self.agent.dispatch(prepared_request)
        for r in agent_response:
            order = self.orders_collection.get_order_by_id(r["order_id"])
            driver = self.drivers_collection.get_by_driver_id(r["driver_id"])
            driver.take_order(order, 0, 0, 0, random.randint(0, 2000))
        # TODO: assign orders to vehicles and vehicles to orders, delete unassigned orders

# if __name__ == '__main__':
#     a = Agent()
#     env = Environment(3, a)
#     idle = env.drivers_collection.get_reposition_drivers(n_drivers=5)
#     env._repositioning(idle)
#     print('hello')
