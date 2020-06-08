from agent import Agent
from simulator import TaxiSimulator

import json

I_INSTALLED_MONGO_CONTAINER = False

if I_INSTALLED_MONGO_CONTAINER:
    taxi_sim = TaxiSimulator(write_simulations_to_db=True, random_seed=2020)
    taxi_sim.simulate(day_of_week=1, agent=Agent(), simulation_name='test_monday')
    simulation = taxi_sim.get_simulation(name='test_monday')
else:
    taxi_sim = TaxiSimulator(write_simulations_to_db=False, random_seed=2020)
    simulation = taxi_sim.simulate(day_of_week=1, agent=Agent())

with open('test_simulation.json', 'w') as f:
    json.dump(simulation, f)
