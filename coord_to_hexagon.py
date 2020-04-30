import pandas as pd
from matplotlib.path import Path

PATH_DATA = "data/hexagon_grid_table.csv"
LON_NAMES = [f"lon{i}" for i in range(1, 7)]
LAT_NAMES = [f"lat{i}" for i in range(1, 7)]
COLUMNS = ["hex"] + [coord for coords in zip(LON_NAMES, LAT_NAMES) for coord in coords]


class CoordToHex:
    def __init__(self, path):
        self.df_hex = pd.read_csv(path, names=COLUMNS)
        self._add_minmax()
        self.d_hex = self._init_dict()

    def _add_minmax(self):
        for coord, names in zip(["lon", "lat"], [LON_NAMES, LAT_NAMES]):
            self.df_hex[f"{coord}_min"] = self.df_hex[names].min(axis=1)
            self.df_hex[f"{coord}_max"] = self.df_hex[names].max(axis=1)

    def _init_dict(self):
        d_hex = {}
        for _, row in self.df_hex.iterrows():
            polygon = [[row[i], row[i + 1]] for i in range(1, 12, 2)]
            path = Path(polygon)
            d_hex[row["hex"]] = path
        return d_hex

    def get_hex(self, lonlat):
        lon, lat = lonlat
        hexagons = self.df_hex.loc[(lon >= self.df_hex["lon_min"]) &
                                   (lon <= self.df_hex["lon_max"]) &
                                   (lat >= self.df_hex["lat_min"]) &
                                   (lat <= self.df_hex["lat_max"]), "hex"]

        for hexagon in hexagons:
            if self.d_hex[hexagon].contains_point([lon, lat]):
                return hexagon

    def get_hex_array(self, lonlat_array):
        hex_array = pd.Series([None] * lonlat_array.shape[0])
        for hexagon, path in self.d_hex.items():
            mask = path.contains_points(lonlat_array)
            hex_array[mask] = hexagon
        return hex_array


if __name__ == "__main__":
    df_req = pd.read_csv("data/total_ride_request/order_20161101",
                         names=["order", "time_start", "time_stop", "pickup_lon", "pickup_lat",
                                "dropoff_lon", "dropoff_lat", "reward"])
    cth = CoordToHex(PATH_DATA)
    df_req["pickup_hex"] = cth.get_hex_array(df_req[["pickup_lon", "pickup_lat"]])

    # EXPERIMENTS
    # df_gps = pd.read_csv("data/gps/gps_20161101", names=["driver", "order", "timestamp", "lon", "lat"])
    # now = datetime.datetime.now()

    # Experiment 1: 200k rows

    # non-vectorized
    # TIME: 180s
    # df_req["pickup_hex"] = df_req[["pickup_lon", "pickup_lat"]].apply(cth.get_hex, axis=1)

    # vectorized
    # TIME: 50s
    # df_req["pickup_hex"] = cth.get_hex_array(df_req[["pickup_lon", "pickup_lat"]])

    # Experiment 2: 2M rows
    # TIME: 420s
    # df_gps = df_gps.iloc[:2*10**6]
    # df_gps["hex"] = cth.get_hex_array(df_gps[["lon", "lat"]])

    # print("Hexagons matched in {:.2f} seconds".format((datetime.datetime.now() - now).total_seconds()))
