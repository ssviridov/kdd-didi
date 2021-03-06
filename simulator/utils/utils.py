from ..models import RewardModel
from geopy.distance import great_circle
import logging

logger = logging.getLogger(__name__)

RMODEL = RewardModel()


def _create_order_driver_pair(env, o, d):
    pair = dict(order_id=o.order_id, driver_id=d.driver_id, order_start_location=o.order_start_location,
                order_finish_location=o.order_finish_location, driver_location=d.driver_location,
                timestamp=env.timestamp, day_of_week=env.day_of_week)
    pair['order_driver_distance'] = get_distance(d.driver_location, o.order_start_location) * 1000
    pair['pick_up_eta'] = pair['order_driver_distance'] / env.PICKUP_SPEED_M_PER_S
    pair['distance'] = env.map.calculate_distance(o.start_hex, o.finish_hex)
    order_duration = pair['distance'] * 1000 / env.PICKUP_SPEED_M_PER_S
    pair['order_finish_timestamp'] = env.timestamp + int(pair['pick_up_eta']) + int(order_duration)
    return pair


def _pairs_for_order(o, env, drivers):
    close_hexes = env.map.d_neighbors[o.start_hex]
    return [_create_order_driver_pair(env, o, d) for d in drivers if d.driver_hex in close_hexes]


def get_distance(start, finish):
    latlon_start = (start[1], start[0])
    latlon_finish = (finish[1], finish[0])
    return great_circle(latlon_start, latlon_finish).km


def prepare_dispatching_request(env, drivers, orders):
    logger.debug("Prepare dispatching request")
    pairs = list()
    logger.debug("Prepare all pairs")
    for order in orders:
        order_pairs = _pairs_for_order(order, env, drivers)
        pairs += [o for o in order_pairs if o['order_driver_distance'] < env.MAX_PICKUP_DISTANCE]
    logger.debug("Prepare reward")
    if len(pairs) == 0:
        return pairs
    else:
        pairs_rewards = list(RMODEL.predict_batch(pairs))
        return [dict(reward_units=rew, **request) for rew, request in zip(pairs_rewards, pairs)]


def handle_dispatching_response(env, agent_request, agent_response):
    logger.debug("Handle dispatching response")
    for r in agent_response:
        order_info = [i for i in agent_request if i['driver_id'] == r['driver_id']
                      and i['order_id'] == r['order_id']][0]
        order = env.orders_collection.get_order_by_id(r["order_id"])
        driver = env.drivers_collection.get_by_driver_id(r["driver_id"])
        driver.take_order(order, reward=order_info['reward_units'], pick_up_eta=order_info['pick_up_eta'],
                          order_finish_timestamp=order_info['order_finish_timestamp'],
                          order_driver_distance=order_info['order_driver_distance'])


class TimeFilter(logging.Filter):

    def filter(self, record):
        try:
            last = self.last
        except AttributeError:
            last = record.relativeCreated

        delta = (record.relativeCreated - last) / 1000

        record.relative = f'{delta:.3f}'

        self.last = record.relativeCreated
        return True
