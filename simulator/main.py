import json
import os
import pylab as plt

from .environment import Environment
from .utils import DataManager

import logging
from logging import config

cur_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(cur_dir, "logging.json"), 'r') as f:
    logging.config.dictConfig(json.load(f))
logger = logging.getLogger(__name__)


class TaxiSimulator:
    def __init__(self, write_simulations_to_db=True, random_seed=None, start_hour: int = 0, end_hour: int = 24):
        assert 0 <= start_hour < end_hour <= 24
        if write_simulations_to_db:
            self.db_client = DataManager()
        else:
            self.db_client = None

        self.start_second = start_hour * 3600 + 1
        self.end_second = end_hour * 3600

        self.random_seed = random_seed

    def simulate(self, day_of_week: int, agent, training_each=60, simulation_name=None):
        if self.db_client:
            self.db_client(simulation_name)
            self.db_client.truncate_training_collection()
        env = Environment(day_of_week=day_of_week, agent=agent, db_client=self.db_client, random_seed=self.random_seed)
        env.generate_orders()
        env.generate_drivers()
        losses = list()
        v_mean = list()
        v_std = list()
        for sec in range(self.start_second, self.end_second + 1):
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
            if sec % training_each == 0:
                loss = env.agent.train()
                if loss:
                    losses.append(loss[0])
                    v_mean.append(loss[1])
                    v_std.append(loss[2])
                    self.plot_losses(losses, v_mean, v_std, sec)
        if not self.db_client:
            return env.datacollector.data
        return env.agent

    def get_simulation(self, name: str):
        return self.db_client.read_simulation(name)

    @staticmethod
    def plot_losses(losses: list, v_mean: list, v_std: list, sec: int):
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(16, 8), sharex=True)

        ax1.plot(losses, color='b')
        ax1.set_ylabel('Loss')

        ax2.plot(v_mean, color='r')
        ax2.set_ylabel('V-mean')

        ax3.plot(v_std, color='g')
        ax3.set_ylabel('V-std')
        ax3.set_xlabel('5mins')

        fig.suptitle(f'{sec} of the day')

        plt.ion()
        plt.show()
