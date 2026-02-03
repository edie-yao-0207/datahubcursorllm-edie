# Databricks notebook source
import time
import requests
import json
import pandas as pd
import geopandas as gpd
from dataclasses import dataclass, field
from shapely.geometry import box
import re

params_here_maps_api = {
    "FC2": {"level": 10},
    "FC3": {"level": 11},
    "FC4": {"level": 12},
    "FC5": {"level": 13},
}


@dataclass
class HereMapAttributes:
    api_key: str
    region: str
    url: str = "https://smap.hereapi.com/v8/maps/attributes"
    default_params: dict = field(init=False)
    region_tiles: dict = field(init=False)
    road_data: dict = field(init=False)
    url_region_tiles: str = "https://naturalearth.s3.amazonaws.com/110m_cultural/ne_110m_admin_0_countries.zip"

    def __post_init__(self):
        """
        The __post_init__ method is a special method in Python dataclasses. It is automatically called immediately
        after the dataclass is initialized. This method allows for additional setup or processing that needs to happen
        after the initial __init__ method is called.
        """
        self.default_params = {"apiKey": self.api_key}
        self.region_tiles = self.calculate_tiles_for_region(self.region)
        attributes_data = self.fetch_attributes(params_here_maps_api)
        road_geometry_data = self.fetch_road_geometry(params_here_maps_api)
        location_data = self.extract_location_data(road_geometry_data)
        merged_data = self.merge_location_and_attributes(location_data, attributes_data)
        self.road_data = self.filter_data(merged_data)

    @staticmethod
    def read_test_attributes():
        attributes = {}
        attributes[key] = pd.read_csv(f"test_data/attributes_data_FC2.csv")
        attributes[key]["HEIGHT_RESTRICTION"] = attributes[key][
            "HEIGHT_RESTRICTION"
        ].astype(str)
        return attributes

    @staticmethod
    def read_test_road_geometry():
        geometry = {key: pd.read_csv(f"test_data/road_geometry_data_FC2.csv")}
        return geometry

    def calculate_tiles_for_region(self, region: str, levels=[10, 11, 12, 13]):
        tiles_result = {}
        world = gpd.read_file(self.url_region_tiles)
        if region == "Europe":
            region_geo = world[world["CONTINENT"] == region]
        elif region in ["Canada", "United States of America", "Mexico"]:
            region_geo = world[world["NAME"] == region]

        geometry = region_geo.union_all()
        for level in levels:
            tile_size = 180 / (2**level)

            def get_tile_id(lon, lat, level):
                tileX = int((lon + 180) / tile_size)
                tileY = int((lat + 90) / tile_size)
                return tileY * 2 * (2**level) + tileX

            minx, miny, maxx, maxy = geometry.bounds
            tiles = set()
            lon = minx
            while lon < maxx:
                lat = miny
                while lat < maxy:
                    tile_box = box(lon, lat, lon + tile_size, lat + tile_size)
                    if geometry.intersects(tile_box):
                        tiles.add(get_tile_id(lon, lat, level))
                    lat += tile_size
                lon += tile_size

            tiles_result[level] = list(tiles)
        return tiles_result

    @staticmethod
    def calculate_tiles(
        west: float, south: float, east: float, north: float, level: int
    ):
        tile_size = 180 / (2**level)
        tileY_south = int((south + 90) / tile_size)
        tileY_north = int((north + 90) / tile_size)
        tileX_west = int((west + 180) / tile_size)
        tileX_east = int((east + 180) / tile_size)

        tile_ids = [
            y * 2 * (2**level) + x
            for y in range(tileY_south, tileY_north + 1)
            for x in range(tileX_west, tileX_east + 1)
        ]
        return tile_ids

    def fetch_and_prepare_data(self, params: dict, retries=3, delay=10):
        attempt = 0
        while attempt < retries:
            try:
                response = requests.get(self.url, params=params)
                response.raise_for_status()  # Raise an error for bad responses
                data = response.json()  # Directly parse JSON
                if data and "Tiles" in data:
                    new_df = pd.json_normalize(data["Tiles"], record_path=["Rows"])
                    return new_df
                return pd.DataFrame()
            except (requests.RequestException, json.JSONDecodeError) as e:
                print(f"Error occurred: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                attempt += 1

    @staticmethod
    def clean_link_id(link_id: str) -> str:
        return link_id.lstrip("-").lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

    def fetch_attributes(self, params_here_maps_api: dict, batch_size=50):
        df_attributes = {}

        for layer, config in params_here_maps_api.items():
            config["tile_ids"] = self.region_tiles[config["level"]]
            config["tiles_ids_string"] = ",".join(map(str, config["tile_ids"]))

            tile_batches = [
                config["tile_ids"][i : i + batch_size]
                for i in range(0, len(config["tile_ids"]), batch_size)
            ]

            all_df_layer = pd.DataFrame()

            for i, batch in enumerate(tile_batches):
                request_params = self.default_params.copy()
                request_params["attributes"] = ",".join(
                    ["HEIGHT_RESTRICTION;LINK_IDS" for tile in batch]
                )
                request_params["in"] = f"tile:{','.join(map(str, batch))}"
                request_params["layers"] = ",".join(
                    [f"TRUCK_RESTR_{layer}" for tile in batch]
                )

                df_layer = self.fetch_and_prepare_data(request_params)
                if not df_layer.empty:
                    df_layer = df_layer.dropna(subset=["HEIGHT_RESTRICTION"])
                    df_layer["LINK_IDS"] = df_layer["LINK_IDS"].apply(
                        self.clean_link_id
                    )
                    all_df_layer = pd.concat(
                        [all_df_layer, df_layer], ignore_index=True
                    )

            df_attributes[layer] = all_df_layer

        return df_attributes

    def fetch_road_geometry(self, params_here_maps_api: dict, batch_size=50):
        data = {}

        for fc in range(2, 6):
            key = f"FC{fc}"
            tile_ids = self.region_tiles[params_here_maps_api[key]["level"]]
            tile_batches = [
                tile_ids[i : i + batch_size]
                for i in range(0, len(tile_ids), batch_size)
            ]

            all_tiles_data = []
            for batch in tile_batches:
                request_params = self.default_params.copy()
                request_params["layers"] = ",".join(
                    [f"ROAD_GEOM_{key}" for tile in batch]
                )
                request_params["in"] = f"tile:{','.join(map(str, batch))}"

                response = requests.get(self.url, params=request_params)
                response_data = json.loads(response.text)
                if "Tiles" in response_data:
                    all_tiles_data.extend(response_data["Tiles"])

            data[key] = all_tiles_data

        return data

    @staticmethod
    def extract_location_data(road_geometry_data: dict) -> dict:
        merged_location_data = {}

        for key, tile_data in road_geometry_data.items():
            all_rows = []
            for tile in tile_data:
                all_rows.extend(tile.get("Rows", []))
            merged_location_data[key] = pd.json_normalize(all_rows)

        return merged_location_data

    @staticmethod
    def transform_lat_lon_multiple(value: str) -> list:
        values = value.split(",")
        values = [float(val) if val != "" else 0 for val in values]
        first_number = values[0]
        coordinates = [first_number / 100000]

        for delta in values[1:]:
            first_number += delta
            coordinates.append(first_number / 100000)

        return coordinates

    def merge_location_and_attributes(
        self, location_data: dict, attributes_data: dict
    ) -> dict:
        merged_dfs_line = {}

        for key in location_data.keys():
            location = location_data[key]

            attributes = attributes_data.get(key, pd.DataFrame())

            merged_df = pd.merge(
                location,
                attributes,
                left_on="LINK_ID",
                right_on="LINK_IDS",
                how="inner",
                indicator=True,
            )

            merged_df["LAT"] = merged_df["LAT"].apply(self.transform_lat_lon_multiple)
            merged_df["LON"] = merged_df["LON"].apply(self.transform_lat_lon_multiple)
            merged_df["HEIGHT_RESTRICTION"] = merged_df["HEIGHT_RESTRICTION"].apply(
                self.convert_height_here_maps
            )
            merged_df["Source"] = key
            merged_dfs_line[key] = merged_df

        return merged_dfs_line

    @staticmethod
    def convert_height_here_maps(height_str: str) -> float:
        if re.match(r"^\d{3}$", height_str):
            return float(height_str) / 100

        # Check if the value is in the format 5'10" or 5' 10"
        feet_inches_match = re.match(r"^(\d+)'(?:\s*(\d+)(?:''|\")?)?$", height_str)
        if feet_inches_match:
            feet = float(feet_inches_match.group(1))
            inches = (
                float(feet_inches_match.group(2)) if feet_inches_match.group(2) else 0
            )
            return (feet + inches / 12) * 0.3048

        return None

    def filter_data(self, merged_dfs_line: dict) -> pd.DataFrame:
        combined_df = pd.concat(merged_dfs_line.values(), ignore_index=True)
        filtered_df = combined_df.query("(BRIDGE != 'N') | (TUNNEL != 'N')")
        filtered_df = filtered_df[
            [
                "Source",
                "NAME",
                "HEIGHT_RESTRICTION",
                "TUNNEL",
                "BRIDGE",
                "LAT",
                "LON",
                "TOPOLOGY_ID",
                "LINK_IDS",
            ]
        ]
        filtered_df = filtered_df.reset_index(drop=True)

        return filtered_df
