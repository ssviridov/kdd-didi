from .driver import DriversCollection
from .order import OrdersCollection
from .agent import Agent
from .map import Map


class Environment:
    def __init__(self, day_of_week: int, agent: Agent):
        self.day_of_week = day_of_week
        self.hour = 0
        self.seconds = 0
        self.minutes = 0

        self.drivers_collection = DriversCollection(env=self)
        self.orders_collection = OrdersCollection(env=self)
        self.map = Map()

        self.agent = agent
        self.total_reward = 0

        # Init some drivers
        # Somehow calculate number of drivers for init
        self.drivers_collection.generate_drivers(n_drivers=self._n_init_drivers())

        # self.generate_orders_for_day()

    def update_current_time(self, current_seconds):
        self.hour = current_seconds//(60*60)
        self.minutes = (current_seconds - self.hour * 60 * 60) // 60
        self.seconds = current_seconds - self.hour * 60 * 60 - self.minutes * 60

    def reposition_actions(self):
        all_idle_drivers = self.drivers_collection.get_drivers('idle')

        # Valid for Repositioning & Agent Repositioning Selection models
        repositioning_drivers = self.drivers_collection.get_reposition_drivers(n_drivers=5)
        idle_drivers = [i for i in all_idle_drivers if i not in repositioning_drivers]

        # Driver repositioning Model
        self._repositioning(repositioning_drivers)

        # Idle Drivers Movement Model (just assigning Driver.next_idle_location without move())
        self._idle_movement(idle_drivers)

    def generate_orders(self):
        # todo: return pre calculated orders for current second
        n_orders = self._n_generate_orders()
        self.orders_collection.generate_orders(n_orders=n_orders)

    def balancing_drivers(self):
        # TODO balance number of drivers in system by deleting and generating new ones
        pass

    def dispatching_actions(self):
        all_idle_drivers = self.drivers_collection.get_dispatching_drivers()
        orders = self.orders_collection.get_orders()

        # All idle drivers all eligible for dispatching
        # Order-Driver Matching Model
        self._dispatching(orders=orders, drivers=all_idle_drivers)

    def cancel_orders(self):
        # TODO use order cancellation model for cancelling order
        orders_to_cancel = []
        self.orders_collection.cancel_orders(orders_to_cancel)

    def move_drivers(self):
        self.drivers_collection.move_drivers()

    # def _n_init_drivers(self):
    #     # TODO calculate number of drivers for initialization
    #     return 100

    # def _n_generate_orders(self):
    #     # TODO: calculate how much orders should be generated at the moment
    #     return 100

    def _repositioning(self, drivers_for_reposition: list):
        # TODO prepare request for agent.reposition()
        prepared_request = drivers_for_reposition
        agent_response = self.agent.reposition(prepared_request)
        # TODO assign reposition locations from response to Driver.next_idle_location

    def _idle_movement(self, idle_drivers: list):
        # TODO Use idle transition probability model for assigning next_idle_location
        pass

    def _dispatching(self, orders, drivers):
        # TODO: prepare request for agent.dispatch()
        prepared_request = [orders, drivers]
        agent_response = self.agent.dispatch(prepared_request)
        # TODO: assign orders to vehicles and vehicles to orders, delete unassigned orders





