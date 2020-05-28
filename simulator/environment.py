import datetime as dt

from driver import DriversCollection
from order import OrdersCollection
from agent import Agent
from map import Map
from models.order_generator import OrderGenerator
from datetime import timedelta


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

        self.df_orders = self._generate_orders_for_day()

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

    def _generate_orders_for_day(self):
        order_gen = OrderGenerator()
        return order_gen.generate_orders(weekday=self.day_of_week)

    def generate_orders(self):
        orders = self.df_orders.loc[
            self.df_orders["order_time"] == timedelta(hours=self.hours, minutes=self.minutes, seconds=self.seconds)]
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
        # TODO use order cancellation model for cancelling order
        orders_to_cancel = []
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
        prepared_request = [orders, drivers]
        agent_response = self.agent.dispatch(prepared_request)
        # TODO: assign orders to vehicles and vehicles to orders, delete unassigned orders

# if __name__ == '__main__':
#     a = Agent()
#     env = Environment(3, a)
#     idle = env.drivers_collection.get_reposition_drivers(n_drivers=5)
#     env._repositioning(idle)
#     print('hello')
