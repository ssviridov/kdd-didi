from agent import Agent
from simulator import TaxiSimulator

taxi_sim = TaxiSimulator()
taxi_sim.simulate(day_of_week=1, agent=Agent())
