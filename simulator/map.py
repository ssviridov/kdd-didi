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

    def sample_driver_location(self):
        # TODO implement model for selecting hexagon for driver location
        hexagon = np.random.choice(self.coords_df.hex.unique())
        return hexagon

    def generate_order_endpoints(self, start_hex, end_hex):
        start_coords = self.coords_df.loc[self.coords_df.hex == start_hex]
        end_coords = self.coords_df.loc[self.coords_df.hex == end_hex]
        return np.random.choice(start_coords.coord), np.random.choice(end_coords.coord)

    def get_grid(self, lonlat: list):
        return self.coords_df.loc[self.coords_df.str_coord == str(lonlat)].hex.values[0]

    def calculate_path(self, start_hex, destination_hex):
        distance, path = nx.single_source_dijkstra(self.graph, start_hex, destination_hex)
        distributed_distance = np.linspace(0, distance, len(path)) * 1000 / self.env.IDLE_SPEED_M_PER_S + self.env.t
        return {i: j for i, j in zip(distributed_distance.astype(int), path)}

# if __name__=='__main__':
#     t = Map()
#     print(t.get_grid(3))
