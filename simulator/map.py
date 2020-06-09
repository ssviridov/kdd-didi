import numpy as np
import pandas as pd
import os
import pickle
import networkx as nx

import logging

logger = logging.getLogger(__name__)

cur_dir = os.path.dirname(os.path.abspath(__file__))


class Map:
    def __init__(self, env, random_seed=None):
        self.env = env
        if random_seed:
            np.random.seed(random_seed)

        with open(os.path.join(cur_dir, 'data', 'hex_graph.pickle'), 'rb') as f:
            self.graph = pickle.load(f)
        with open(os.path.join(cur_dir, 'data', 'd_paths.pickle'), 'rb') as f:
            self.d_paths = pickle.load(f)

        self.coords_df = pd.read_csv(os.path.join(cur_dir, 'data', 'coords_hex.csv'), sep=';')

        # filter nodes
        hexes = pd.read_csv(os.path.join(cur_dir, 'data', 'hexes.csv'), sep=';')
        self.graph = self.graph.subgraph(hexes["hex"])
        self.coords_df = self.coords_df.loc[self.coords_df["hex"].isin(hexes["hex"])]

        # create dict from df for faster access
        self.d_coords = {}
        for _, row in self.coords_df.iterrows():
            self.d_coords[row["hex"]] = row[["lon_min", "lon_max", "lat_min", "lat_max"]]

        # get neighbors of neighbors
        self.d_neighbors = {}
        for n in self.graph:
            self.d_neighbors[n] = set(neigh_neigh_node
                                      for neigh_node in self.graph.neighbors(n)
                                      for neigh_neigh_node in self.graph.neighbors(neigh_node))

    def get_lonlat(self, hexagon):
        hex_row = self.d_coords[hexagon]
        return self._generate_coord(hex_row)

    def generate_order_endpoints(self, start_hex, end_hex):
        start_row = self.d_coords[start_hex]
        end_row = self.d_coords[end_hex]
        start_location = self._generate_coord(start_row)
        end_location = self._generate_coord(end_row)
        return start_location, end_location

    def calculate_path(self, start_hex, destination_hex):
        try:
            distance, path = self.d_paths[(start_hex, destination_hex)]
        except KeyError:
            distance, path = nx.single_source_dijkstra(self.graph, start_hex, destination_hex)
        distributed_distance = np.linspace(0, distance, len(path)) * 1000 / self.env.IDLE_SPEED_M_PER_S + self.env.t
        return {i: j for i, j in zip(distributed_distance.astype(int), path)}

    def calculate_distance(self, start_hex, finish_hex):
        try:
            distance, _ = self.d_paths[(start_hex, finish_hex)]
        except KeyError:
            distance, _ = nx.single_source_dijkstra(self.graph, start_hex, finish_hex)
        return distance

    @staticmethod
    def _generate_coord(row):
        return [np.random.uniform(row.lon_min, row.lon_max),
                np.random.uniform(row.lat_min, row.lat_max)]

# if __name__=='__main__':
#     t = Map()
#     print(t.get_grid(3))
