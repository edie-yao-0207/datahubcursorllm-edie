# Databricks notebook source
import builtins
import math
import re
from typing import List

import h3
import numpy as np
import shapely
from pyspark.sql.functions import (
    array,
    broadcast,
    coalesce,
    col,
    collect_list,
    collect_set,
    concat_ws,
    explode,
    flatten,
    lit,
    row_number,
    size,
    split,
    udf,
)
from pyspark.sql.types import *
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from shapely import wkt
from shapely.geometry import LineString


class CoordinateUtils:
    @staticmethod
    def coordinates_to_wkt(lat_list, lon_list):
        # Converts lists of latitudes and longitudes to a WKT LineString.
        points = zip(lon_list, lat_list)
        line = LineString(points)
        return line.wkt

    @staticmethod
    def extract_coordinates_to_points(lat_list, lon_list):
        points = list(zip(lon_list, lat_list))
        return points

    @staticmethod
    def extract_and_sort_lat_lon(nodes):
        # Extracts and sorts latitudes and longitudes from a list of nodes.
        sorted_nodes = sorted(nodes, key=lambda x: x["ind"])
        latitudes = [node["latitude"] for node in sorted_nodes]
        longitudes = [node["longitude"] for node in sorted_nodes]
        return latitudes, longitudes

    @staticmethod
    def extract_locale_from_coordinates(coordinates):
        # Returns the locale based on the first node's latitude and longitude.
        if not coordinates:
            return ""

        first_coord = coordinates[0]
        lat = first_coord[1]
        lon = first_coord[0]

        # Define locale boundaries
        locale_boundaries = {
            "UK": {
                "lat_min": 49.97,
                "lat_max": 59.15456,
                "lon_min": -10.53,
                "lon_max": 1.812,
            },
        }

        for locale, boundaries in locale_boundaries.items():
            if (
                boundaries["lat_min"] <= lat <= boundaries["lat_max"]
                and boundaries["lon_min"] <= lon <= boundaries["lon_max"]
            ):
                return locale

        return ""

    @staticmethod
    def calculate_length_from_nodes(nodes):
        # Calculates the length of a path defined by a list of nodes using the Haversine formula.
        coords = [
            (node["longitude"], node["latitude"])
            for node in sorted(nodes, key=lambda x: x["ind"])
        ]
        length = sum(
            CoordinateUtils.haversine(coords[i], coords[i + 1])
            for i in range(len(coords) - 1)
        )
        return round(length, 1)

    @staticmethod
    def extract_node_ids(nodes):
        # Extracts node IDs from a list of nodes.
        return [node["nodeId"] for node in nodes]

    @staticmethod
    def line_string_linear_interp(points_wkt: str, step: float = 0.00008) -> str:
        # Interpolates points along a LineString defined by WKT.
        if not isinstance(points_wkt, str):
            raise ValueError(
                f"Input data must be a string in WKT format. {type(points_wkt)}, {points_wkt}"
            )
        geom = shapely.wkt.loads(points_wkt)
        x, y = geom.xy

        xvals = []
        yvals = []

        for i in range(len(x) - 1):
            point_count = max(
                int(np.round(np.abs((x[i + 1] - x[i])) / step)),
                int(np.round(np.abs((y[i + 1] - y[i])) / step)),
            )
            if point_count <= 1:
                point_count = 2

            seg_x_vals = np.linspace(x[i], x[i + 1], point_count, endpoint=False)
            seg_y_vals = np.linspace(y[i], y[i + 1], point_count, endpoint=False)
            xvals.extend(seg_x_vals)
            yvals.extend(seg_y_vals)

        xvals.append(x[-1])
        yvals.append(y[-1])

        line_string = shapely.geometry.LineString(zip(xvals, yvals))
        return line_string.wkt

    @staticmethod
    def wkt_to_points(wkt_string):
        # Converts a WKT string to a list of points.
        try:
            geometry = shapely.wkt.loads(wkt_string)
            return list(geometry.coords)
        except Exception as e:
            print(f"Error converting WKT to points: {wkt_string} - {e}")
            return []

    @staticmethod
    def haversine(coord1, coord2):
        # Calculates the Haversine distance between two coordinates.
        R = 6371000
        lon1, lat1, lon2, lat2 = map(
            math.radians, [coord1[0], coord1[1], coord2[0], coord2[1]]
        )
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    @staticmethod
    def calculate_length_haversine(wkt_str):
        # Calculates the length of a LineString defined by WKT using the Haversine formula.
        line = wkt.loads(wkt_str)
        coords = list(line.coords)
        length = sum(
            CoordinateUtils.haversine(coords[i], coords[i + 1])
            for i in range(len(coords) - 1)
        )
        return round(length, 1)

    @staticmethod
    def calculate_length_from_coordinates(coords):
        total_distance = 0.0
        for i in range(len(coords) - 1):
            total_distance += CoordinateUtils.haversine(coords[i], coords[i + 1])
        return round(total_distance, 2)

    @staticmethod
    def line_string_to_h3_str(geom: str, res: int) -> List[str]:
        """Converts points in a linestring to a series of h3 buckets for map-matching."""
        h3_indices = set()
        if not isinstance(geom, str):
            raise ValueError(
                f"Input data geom must be a string in WKT format. {type(geom)}, {geom}"
            )
        geom = shapely.wkt.loads(geom)
        for (lng, lat) in geom.coords[:]:
            h3_idx = h3.geo_to_h3(lat, lng, res)
            h3_indices.add(h3_idx)
        return list(h3_indices)

    @staticmethod
    def get_lat_lng_from_visited_nodes(visited_nodes, all_nodes, all_nodes_coordinates):
        lat_lng_list = []
        for node in visited_nodes:
            try:
                index = all_nodes.index(node)
                coords = all_nodes_coordinates[index]
                lat_lng_list.append(coords)
            except ValueError:
                continue

        return lat_lng_list if lat_lng_list else [[]]

    @staticmethod
    def get_subset_by_edges(nodes, edge1, edge2):
        node_to_index = {node: i for i, node in enumerate(nodes)}
        try:
            start = min(node_to_index[edge1], node_to_index[edge2])
            end = max(node_to_index[edge1], node_to_index[edge2])
            return nodes[start : end + 1]
        except KeyError:
            return []


class TagUtils:
    highways_without_max_height = ["service"]
    max_height_tags = [
        "maxheight",
        "maxheight:physical",
        "maxheight:legal",
        "maxheight:m",
        "maxheight:imperial",
        "maxheight:ft",
    ]
    locale_height_map = {"UK": 5}

    # Ways that hold specific tags and values will be not taken into account as bridge ways.
    filter_out_tags = {}

    @staticmethod
    def extract_oneway(tags):
        # Extracts and interprets the oneway tag from a list of tags
        for tag in tags:
            if tag["key"] == "oneway":
                if tag["value"].lower() == "yes":
                    return 1
                elif tag["value"] == "-1":
                    return -1
        return 0

    @staticmethod
    def get_exit_node(oneway, nodes):
        """
        Determines the exit node based on the oneway status and the list of nodes.

        Args:
            oneway (int): The oneway status of the road. 1 for forward direction, -1 for reverse, 0 for two-way.
            nodes (list): The list of nodes representing the road.

        Returns:
            The exit node, which is either the last or first node in the list, or None if the way is not one-way.
        """
        if not nodes:
            return None

        if oneway == 1:
            return nodes[-1]
        elif oneway == -1:
            return nodes[0]  # For roads with oneway=-1, the first node is the exit node
        else:
            return None

    @staticmethod
    def extract_maxheight(tags, locale=None):
        # Define a map for locale to default max height

        # Set maxheight to None if the highway is in the list of excluded highways
        for tag in tags:
            if tag["key"] == "highway":
                if tag["value"] in TagUtils.highways_without_max_height:
                    return None
                else:
                    break

        # Extracts and converts the maxheight tag from a list of tags.
        for tag in tags:
            if tag["key"] in TagUtils.max_height_tags:
                height_str = tag["value"]
                height_str = height_str.replace(" ", "")
                if (
                    height_str.lower() == "default"
                    and locale in TagUtils.locale_height_map
                ):
                    return TagUtils.locale_height_map[locale]
                # Check for meters (either just a number, or ends with 'm' or 'meters')
                meter_match = re.match(
                    r"^(\d+(\.\d+)?)(m|meters?)?$", height_str, re.IGNORECASE
                )
                if meter_match:
                    return float(meter_match.group(1))
                # Matches strings like 5', 5'10", 5' 10", or 16'3"
                feet_inches_match = re.match(
                    r"^(\d+)'(?:\s*(\d+)(?:''|\"))?$", height_str
                )
                if feet_inches_match:
                    feet = float(feet_inches_match.group(1))
                    inches = (
                        float(feet_inches_match.group(2))
                        if feet_inches_match.group(2)
                        else 0
                    )
                    return (feet + inches / 12) * 0.3048
        return None

    @staticmethod
    def convert_external_max_height(height_str, locale=None):
        if height_str is None:
            return None

        if height_str.lower().strip() == "default":
            if locale in TagUtils.locale_height_map:
                return TagUtils.locale_height_map[locale]
            else:
                return None

        height_str = height_str.replace(" ", "").lower()

        meter_match = re.match(
            r"^(\d+(\.\d+)?)(m|meters?)?$", height_str, re.IGNORECASE
        )
        if meter_match:
            return float(meter_match.group(1))

        feet_inches_match = re.match(r"^(\d+)'(?:(\d+)(?:''|\"))?$", height_str)
        if feet_inches_match:
            feet = float(feet_inches_match.group(1))
            inches = 0.0
            if feet_inches_match.group(2):
                inches = float(feet_inches_match.group(2))
            return (feet + inches / 12) * 0.3048
        return None

    @staticmethod
    def extract_special_tags(tags):
        # Extracts special tags (maxheight, bridge, tunnel) from a list of tags.
        special_tags = []
        for tag in tags:
            if tag["key"] in ["maxheight", "bridge", "tunnel"]:
                special_tags.append(tag["key"])

        return special_tags

    @staticmethod
    def tag_filter(tags):
        # Check if any key-value pair in filter_out_tags exists in the tags
        for key, value in TagUtils.filter_out_tags.items():
            for tag in tags:
                if tag.get("key") == key and tag.get("value") == value:
                    return True
        return False

    @staticmethod
    def count_unique_tags(tags):
        # Counts the number of unique tags in a list of tags.
        unique_tags = set(tags)
        return len(unique_tags)


class HeightUtils:
    @staticmethod
    def parse_height_to_meters(height_str):
        # Converts height from centimeters to meters.
        if height_str is None:
            return None
        return float(height_str) * 0.01

    @staticmethod
    def convert_height_here_maps(height_str):
        # Converts height from various formats to meters.
        # Check if the value is a three-digit number (interpreted as cm)
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


count_unique_tags_udf = udf(TagUtils.count_unique_tags, IntegerType())
interp_udf = udf(CoordinateUtils.line_string_linear_interp, StringType())
wkt_udf = udf(CoordinateUtils.coordinates_to_wkt, StringType())
extract_coordinates_to_points_udf = udf(
    CoordinateUtils.extract_coordinates_to_points,
    ArrayType(
        StructType(
            [
                StructField("lon", DoubleType(), True),
                StructField("lat", DoubleType(), True),
            ]
        )
    ),
)
line_string_to_h3_str_udf = udf(
    lambda geom, res: CoordinateUtils.line_string_to_h3_str(geom, res),
    ArrayType(StringType()),
)
extract_locale_from_coordinates_udf = udf(
    CoordinateUtils.extract_locale_from_coordinates, StringType()
)
extract_oneway_udf = udf(TagUtils.extract_oneway, IntegerType())
extract_exit_node_udf = udf(TagUtils.get_exit_node, LongType())
extract_maxheight_udf = udf(TagUtils.extract_maxheight, StringType())
extract_special_tags_udf = udf(TagUtils.extract_special_tags, ArrayType(StringType()))
tag_filter_udf = udf(TagUtils.tag_filter, BooleanType())
calculate_length_udf = udf(CoordinateUtils.calculate_length_from_nodes, DoubleType())
extract_node_ids_udf = udf(CoordinateUtils.extract_node_ids, ArrayType(LongType()))
extract_and_sort_lat_lon_udf = udf(
    CoordinateUtils.extract_and_sort_lat_lon,
    StructType(
        [
            StructField("lat", ArrayType(DoubleType()), True),
            StructField("lon", ArrayType(DoubleType()), True),
        ]
    ),
)
is_in_bbox_udf = udf(lambda nodes: CoordinateUtils.is_in_bbox(nodes), BooleanType())
convert_height_here_maps_udf = udf(HeightUtils.convert_height_here_maps, FloatType())
parse_height_to_meters_udf = udf(HeightUtils.parse_height_to_meters, FloatType())
get_subset_by_edges_udf = udf(
    CoordinateUtils.get_subset_by_edges, ArrayType(LongType())
)
get_lat_lng_from_visited_nodes_udf = udf(
    CoordinateUtils.get_lat_lng_from_visited_nodes, ArrayType(ArrayType(FloatType()))
)
calculate_length_from_coordinates_udf = udf(
    CoordinateUtils.calculate_length_from_coordinates, DoubleType()
)
convert_external_max_height_udf = udf(
    TagUtils.convert_external_max_height, StringType()
)
