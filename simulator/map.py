import numpy as np
import pandas as pd
import os
import pickle
import networkx as nx

cur_dir = os.path.dirname(os.path.abspath(__file__))

class Map:
    def __init__(self, env):
        self.env = env
        with open(os.path.join(cur_dir, 'data', 'hex_graph.pickle'), 'rb') as f:
            self.graph = pickle.load(f)

        self.coords_df = pd.read_csv(os.path.join(cur_dir, 'data', 'coords_hex.csv'))
        pass

    def sample_driver_location(self):
        # TODO implement model for selecting hexagon for driver location
        hexagon = np.random.choice(self.coords_df.hex.unique())
        coords = self.coords_df.loc[self.coords_df.hex == hexagon]
        return np.random.choice(coords.coord)

    def generate_order_endpoints(self):
        # TODO implement order_generation model for select start-end hexagons for order
        start_hex, end_hex = np.random.choice(self.coords_df.hex.unique()), \
                             np.random.choice(self.coords_df.hex.unique())
        start_coords = self.coords_df.loc[self.coords_df.hex == start_hex]
        end_coords = self.coords_df.loc[self.coords_df.hex == end_hex]
        return np.random.choice(start_coords.coord), np.random.choice(end_coords.coord)

    def get_grid(self, lonlat: list):
        return self.coords_df.loc[self.coords_df.str_coord == str(lonlat)].hex.values[0]

    def calculate_idle_path(self):
        return [{}, {}]

# if __name__=='__main__':
#     t = Map()
#     print(t.get_grid(3))