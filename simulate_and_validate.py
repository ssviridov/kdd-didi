from simulator import TaxiSimulator
from simulator.agent import Agent
from simulator.utils import DataManager

from rl.agents import ValueAgent
from rl.models import ValueNetwork
from rl.buffers import MongoDBReplayBuffer, PostgreSQLReplayBuffer

from random import random
import torch
import pandas as pd
import sys
import os
# from submit3.agent import Agent as SubmissionAgent

cur_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(cur_dir, 'rl'))

SIMULATION_NAME = 'baseline_agent_monday'

dirname = os.path.join(cur_dir, 'validation', SIMULATION_NAME)
if not os.path.exists(dirname):
    os.makedirs(dirname)

if __name__ == '__main__':
    buffer = PostgreSQLReplayBuffer()
    agent = ValueAgent(replay_buffer=buffer, value_net_class=ValueNetwork)
    agent.value_net = torch.load(os.path.join(cur_dir, 'model_bs256_g92_u10.pth'), map_location=torch.device('cpu'))
    agent.value_net.eval()

    simulator = TaxiSimulator(random_seed=42)

    simulation_name = SIMULATION_NAME + str(random())

    intelligent_agent = simulator.simulate(day_of_week=1, agent=agent,
                                           simulation_name=simulation_name, training_each=1000000)
    dbclient = DataManager()
    simulation = dbclient.read_simulation(simulation_name)
    total = pd.DataFrame([i['total'] for i in simulation]).to_csv(os.path.join(dirname, 'total.csv'), index=False, sep=';')

    if hasattr(intelligent_agent, 'value_net'):
        intelligent_agent.save(os.path.join(dirname, 'model.pth'))

