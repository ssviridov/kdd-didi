from simulator import TaxiSimulator

from rl.agents import ValueAgent
from rl.models import ValueNetwork
from rl.buffers import MongoDBReplayBuffer

from random import random

if __name__ == '__main__':
    buffer = MongoDBReplayBuffer()
    agent = ValueAgent(replay_buffer=buffer, value_net_class=ValueNetwork)
    simulator = TaxiSimulator()

    simulation_name = 'thursday_' + str(random())

    intelligent_agent = simulator.simulate(day_of_week=4, agent=agent,
                                           simulation_name=simulation_name, training_each=300)
