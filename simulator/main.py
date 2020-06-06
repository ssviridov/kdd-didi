import json
import os

from .environment import Environment
from .agent import Agent
from .utils import DataManager

import logging
from logging import config

cur_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(cur_dir, "logging.json"), 'r') as f:
    logging.config.dictConfig(json.load(f))
logger = logging.getLogger(__name__)


class TaxiSimulator:
    day_seconds = 60 * 60 * 24

    def __init__(self, write_simulations_to_db=True):
        if write_simulations_to_db:
            self.db_client = DataManager()
        else:
            self.db_client = None

    def simulate(self, day_of_week: int, agent: Agent, simulation_name=None):
        if self.db_client:
            self.db_client(simulation_name)
        env = Environment(day_of_week=day_of_week, agent=agent, db_client=self.db_client)
        env.generate_orders()
        env.generate_drivers()
        for sec in range(1, self.day_seconds + 1):
            env.update_current_time(current_seconds=sec)
            if sec % 100 == 0:
                env.reposition_actions()
            env.get_orders_for_second()
            env.balancing_drivers()
            env.idle_movement()
            if sec % 2 == 0:
                env.dispatching_actions()
            env.move_drivers()
            env.datacollector.write_simulation_step()
        if not self.db_client:
            return env.datacollector.data

    def get_simulation(self, name: str):
        return self.db_client.read_simulation(name)
