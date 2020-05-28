from .environment import Environment
from .agent import Agent


class TaxiSimulator:
    day_seconds = 60 * 60 * 24

    def __init__(self):
        pass

    def simulate(self, day_of_week: int, agent: Agent):
        env = Environment(day_of_week=day_of_week, agent=agent)
        for sec in range(1, self.day_seconds + 1):
            env.update_current_time(current_seconds=sec)
            if sec % 100 == 0:
                env.reposition_actions()
            env.generate_orders()
            env.balancing_drivers()
            if sec % 2 == 0:
                env.dispatching_actions()
                env.cancel_orders()
            env.move_drivers()
