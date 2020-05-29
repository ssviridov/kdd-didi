import json

from .environment import Environment
from .agent import Agent

import logging
from logging import config

with open("logging.json", 'r') as f:
    logging.config.dictConfig(json.load(f))
logger = logging.getLogger(__name__)


class TaxiSimulator:
    day_seconds = 60 * 60 * 24

    def __init__(self):
        pass

    def simulate(self, day_of_week: int, agent: Agent):
        env = Environment(day_of_week=day_of_week, agent=agent)
        env.generate_orders()
        for sec in range(1, self.day_seconds + 1):
            env.update_current_time(current_seconds=sec)
            if sec % 100 == 0:
                env.reposition_actions()
            env.get_orders_for_second()
            env.balancing_drivers()
            if sec % 2 == 0:
                env.dispatching_actions()
                env.cancel_orders()
            env.move_drivers()
