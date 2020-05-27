import itertools


class OrderException(Exception):
    pass


class OrdersCollectionException(Exception):
    pass


class Order:
    newid = itertools.count()
    status_list = ['assigned', 'unassigned']

    def __init__(self, env):
        self.env = env
        self.order_id = next(self.newid)
        self.order_start_location, self.order_finish_location = self.env.map.generate_order_endpoints()
        self.status = 'unassigned'
        self.vehicle = None
        self.reward = None
        self.pick_up_eta = None
        self.order_finish_timestamp = None
        self.order_driver_distance = None

    def assigning(self, vehicle, reward: float, pick_up_eta: float,
                  order_finish_timestamp: int, order_driver_distance: float):
        self.vehicle = vehicle
        self.reward = reward
        self.pick_up_eta = pick_up_eta
        self.order_finish_timestamp = order_finish_timestamp
        self.order_driver_distance = order_driver_distance
        self.status = 'assigned'

    def cancel(self):
        self.vehicle.cancel_order()


class OrdersCollection(list):
    def __init__(self, env):
        self.env = env
        super().__init__()

    def generate_orders(self, n_orders: int):
        for _ in range(n_orders):
            self.append(Order(env=self.env))

    def get_orders(self, status: str):
        if status not in Order.status_list:
            raise OrdersCollectionException(f'status must be one of {Order.status_list}')
        return [i for i in self if i.status == status]

    def delete_unassigned_orders(self):
        for order in self:
            if order.status == 'unassigned':
                self.remove(order)

    def cancel_orders(self, orders_list: list):
        for order in orders_list:
            order.cancel()
            self.remove(order)
