import numpy as np
import pandas as pd
import os
import pickle
import networkx as nx

import logging

logger = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(__file__))


class Map:
    def __init__(self, env):
        self.env = env
        with open(os.path.join(cur_dir, 'data', 'hex_graph.pickle'), 'rb') as f:
            self.graph = pickle.load(f)

        self.coords_df = pd.read_csv(os.path.join(cur_dir, 'data', 'coords_hex.csv'), sep=';')

    def sample_driver_location(self, start_hex):
        if start_hex is None:
            hexagon = np.random.choice(self.coords_df.hex.unique())
        else:
            hexagon = start_hex
        lonlat = self.get_lonlat(hexagon)
        return hexagon, lonlat

    def get_lonlat(self, hexagon):
        hex_row = self.coords_df.loc[self.coords_df.hex == hexagon].reset_index().iloc[0]
        return self._generate_coord(hex_row)

    def generate_order_endpoints(self, start_hex, end_hex):
        # logger.info(f"Start loc start_coords")
        start_row = self.coords_df.loc[self.coords_df.hex == start_hex].reset_index().iloc[0]
        # logger.info(f"Start loc stop_coords")
        end_row = self.coords_df.loc[self.coords_df.hex == end_hex].reset_index().iloc[0]
        # logger.info(f"Start random choice")
        start_location = self._generate_coord(start_row)
        end_location = self._generate_coord(end_row)
        return start_location, end_location

    def calculate_path(self, start_hex, destination_hex):
        distance, path = nx.single_source_dijkstra(self.graph, start_hex, destination_hex)
        distributed_distance = np.linspace(0, distance, len(path)) * 1000 / self.env.IDLE_SPEED_M_PER_S + self.env.t
        return {i: j for i, j in zip(distributed_distance.astype(int), path)}

    def calculate_distance(self, start_hex, finish_hex):
        distance, _ = nx.single_source_dijkstra(self.graph, start_hex, finish_hex)
        return distance

    @staticmethod
    def _generate_coord(row):
        return [np.random.uniform(row.lon_min, row.lon_max),
                np.random.uniform(row.lat_min, row.lat_max)]

# if __name__=='__main__':
#     t = Map()
#     print(t.get_grid(3))
