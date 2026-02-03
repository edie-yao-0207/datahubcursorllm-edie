# Databricks notebook source
# MAGIC %pip install shapely
# MAGIC %pip install h3
# MAGIC %pip install geopandas

# COMMAND ----------

import os

os.chdir("/Workspace/backend/safety/low_bridge_strikes/")

# COMMAND ----------

# MAGIC %run ./udf_helpers

# COMMAND ----------

# MAGIC %run ./dangerous_ways_processor

# COMMAND ----------

from builtins import min, max
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    ArrayType,
    FloatType,
)

schema_ways = StructType(
    [
        StructField("way_id", IntegerType(), True),
        StructField("nodes", ArrayType(LongType()), True),
        StructField("max_height", IntegerType(), True),
        StructField("length", IntegerType(), True),
        StructField("coordinates", ArrayType(ArrayType(DoubleType())), True),
    ]
)

schema_nodes = StructType(
    [
        StructField("node_id", LongType(), True),
        StructField("way_id", IntegerType(), True),
        StructField("index", IntegerType(), True),
    ]
)

schema_nodes_lat_lng = StructType(
    [
        StructField("way_id", IntegerType(), True),
        StructField("node_lat_lng", MapType(StringType(), DoubleType()), True),
    ]
)


def create_mock_dataframes(spark):
    data_ways = [
        (
            1,
            [100, 101, 102, 103],
            3,
            40,
            [[52.01, 13.04], [52.02, 13.05], [52.03, 13.06], [52.04, 13.07]],
        ),
        (
            2,
            [103, 104, 105, 106],
            None,
            40,
            [[52.04, 13.07], [52.05, 13.08], [52.06, 13.09], [52.07, 14.00]],
        ),
        (
            3,
            [107, 102, 108, 109],
            None,
            50,
            [[52.03, 13.08], [52.03, 13.06], [52.05, 13.05], [52.06, 13.04]],
        ),
        (
            4,
            [110, 108, 111, 112],
            None,
            40,
            [[52.05, 13.04], [52.05, 13.05], [52.07, 13.06], [52.08, 13.07]],
        ),
        (
            5,
            [113, 100, 114, 115],
            None,
            50,
            [[52.00, 13.02], [52.01, 13.04], [52.00, 13.05], [51.09, 13.06]],
        ),
        (
            6,
            [116, 114, 117, 118],
            None,
            40,
            [[51.09, 13.06], [52.00, 13.05], [51.08, 13.07], [51.07, 13.08]],
        ),
        (
            7,
            [119, 113, 120, 121],
            None,
            60,
            [[51.05, 13.00], [52.00, 13.02], [51.04, 13.03], [51.03, 13.04]],
        ),
        (
            8,
            [122, 115, 123, 124],
            None,
            50,
            [[51.02, 13.04], [51.09, 13.06], [51.01, 13.05], [51.00, 13.06]],
        ),
        (
            9,
            [125, 126, 127, 128],
            None,
            40,
            [[52.01, 13.08], [52.02, 13.09], [52.03, 14.00], [52.04, 14.01]],
        ),
        (
            10,
            [129, 128, 130, 131],
            4,
            50,
            [[52.04, 14.01], [52.04, 14.01], [52.05, 13.09], [52.06, 13.08]],
        ),
        (
            11,
            [132, 127, 133, 134],
            None,
            40,
            [[52.03, 14.00], [52.03, 14.00], [52.05, 14.01], [52.06, 14.02]],
        ),
        (
            12,
            [106, 135, 136, 137],
            None,
            40,
            [[52.07, 14.00], [52.03, 14.00], [52.05, 14.01], [52.06, 14.04]],
        ),
        (
            13,
            [137, 138, 139, 140],
            None,
            40,
            [[52.06, 14.04], [52.06, 14.03], [52.06, 14.00], [52.06, 14.01]],
        ),
    ]

    data_nodes = [
        (100, 1, 0),
        (101, 1, 1),
        (102, 1, 2),
        (103, 1, 3),
        (103, 2, 0),
        (104, 2, 1),
        (105, 2, 2),
        (106, 2, 3),
        (107, 3, 0),
        (102, 3, 1),
        (108, 3, 2),
        (109, 3, 3),
        (110, 4, 0),
        (108, 4, 1),
        (111, 4, 2),
        (112, 4, 3),
        (113, 5, 0),
        (100, 5, 1),
        (114, 5, 2),
        (115, 5, 3),
        (116, 6, 0),
        (114, 6, 1),
        (117, 6, 2),
        (118, 6, 3),
        (119, 7, 0),
        (113, 7, 1),
        (120, 7, 2),
        (121, 7, 3),
        (122, 8, 0),
        (115, 8, 1),
        (123, 8, 2),
        (124, 8, 3),
        (125, 9, 0),
        (126, 9, 1),
        (127, 9, 2),
        (128, 9, 3),
        (129, 10, 0),
        (128, 10, 1),
        (130, 10, 2),
        (131, 10, 3),
        (132, 11, 0),
        (127, 11, 1),
        (133, 11, 2),
        (134, 11, 3),
        (106, 12, 0),
        (135, 12, 1),
        (136, 12, 2),
        (137, 12, 3),
        (137, 13, 0),
        (138, 13, 1),
        (139, 13, 2),
        (140, 13, 3),
    ]

    data_nodes_lat_lng = [
        (1, {"ind": 0.0, "latitude": 52.01, "longitude": 13.04}),
        (1, {"ind": 1.0, "latitude": 52.02, "longitude": 13.05}),
        (1, {"ind": 2.0, "latitude": 52.03, "longitude": 13.06}),
        (1, {"ind": 3.0, "latitude": 52.04, "longitude": 13.07}),
        (2, {"ind": 0.0, "latitude": 52.04, "longitude": 13.07}),
        (2, {"ind": 1.0, "latitude": 52.05, "longitude": 13.08}),
        (2, {"ind": 2.0, "latitude": 52.06, "longitude": 13.09}),
        (2, {"ind": 3.0, "latitude": 52.07, "longitude": 14.00}),
        (3, {"ind": 0.0, "latitude": 52.03, "longitude": 13.08}),
        (3, {"ind": 1.0, "latitude": 52.03, "longitude": 13.06}),
        (3, {"ind": 2.0, "latitude": 52.05, "longitude": 13.05}),
        (3, {"ind": 3.0, "latitude": 52.06, "longitude": 13.04}),
        (4, {"ind": 0.0, "latitude": 52.05, "longitude": 13.04}),
        (4, {"ind": 1.0, "latitude": 52.05, "longitude": 13.05}),
        (4, {"ind": 2.0, "latitude": 52.07, "longitude": 13.06}),
        (4, {"ind": 3.0, "latitude": 52.08, "longitude": 13.07}),
        (5, {"ind": 0.0, "latitude": 52.00, "longitude": 13.02}),
        (5, {"ind": 1.0, "latitude": 52.01, "longitude": 13.04}),
        (5, {"ind": 2.0, "latitude": 52.00, "longitude": 13.05}),
        (5, {"ind": 3.0, "latitude": 51.09, "longitude": 13.06}),
        (6, {"ind": 0.0, "latitude": 51.09, "longitude": 13.06}),
        (6, {"ind": 1.0, "latitude": 52.00, "longitude": 13.05}),
        (6, {"ind": 2.0, "latitude": 51.08, "longitude": 13.07}),
        (6, {"ind": 3.0, "latitude": 51.07, "longitude": 13.08}),
        (7, {"ind": 0.0, "latitude": 51.05, "longitude": 13.00}),
        (7, {"ind": 1.0, "latitude": 52.00, "longitude": 13.02}),
        (7, {"ind": 2.0, "latitude": 51.04, "longitude": 13.03}),
        (7, {"ind": 3.0, "latitude": 51.03, "longitude": 13.04}),
        (8, {"ind": 0.0, "latitude": 51.02, "longitude": 13.04}),
        (8, {"ind": 1.0, "latitude": 51.09, "longitude": 13.06}),
        (8, {"ind": 2.0, "latitude": 51.01, "longitude": 13.05}),
        (8, {"ind": 3.0, "latitude": 51.00, "longitude": 13.06}),
        (9, {"ind": 0.0, "latitude": 52.01, "longitude": 13.08}),
        (9, {"ind": 1.0, "latitude": 52.02, "longitude": 13.09}),
        (9, {"ind": 2.0, "latitude": 52.03, "longitude": 14.00}),
        (9, {"ind": 3.0, "latitude": 52.04, "longitude": 14.01}),
        (10, {"ind": 0.0, "latitude": 52.04, "longitude": 14.01}),
        (10, {"ind": 1.0, "latitude": 52.04, "longitude": 14.01}),
        (10, {"ind": 2.0, "latitude": 52.05, "longitude": 13.09}),
        (10, {"ind": 3.0, "latitude": 52.06, "longitude": 13.08}),
        (11, {"ind": 0.0, "latitude": 52.03, "longitude": 14.00}),
        (11, {"ind": 1.0, "latitude": 52.03, "longitude": 14.00}),
        (11, {"ind": 2.0, "latitude": 52.05, "longitude": 14.01}),
        (11, {"ind": 3.0, "latitude": 52.06, "longitude": 14.02}),
        (12, {"ind": 0.0, "latitude": 52.07, "longitude": 14.00}),
        (12, {"ind": 1.0, "latitude": 52.03, "longitude": 14.01}),
        (12, {"ind": 2.0, "latitude": 52.05, "longitude": 14.01}),
        (12, {"ind": 3.0, "latitude": 52.06, "longitude": 14.04}),
        (13, {"ind": 0.0, "latitude": 52.06, "longitude": 14.04}),
        (13, {"ind": 1.0, "latitude": 52.06, "longitude": 14.03}),
        (13, {"ind": 2.0, "latitude": 52.06, "longitude": 14.00}),
        (13, {"ind": 3.0, "latitude": 52.06, "longitude": 14.01}),
    ]

    ways_df = spark.createDataFrame(data_ways, schema=schema_ways)
    nodes_df = spark.createDataFrame(data_nodes, schema=schema_nodes)
    nodes_lat_lng_df = spark.createDataFrame(
        data_nodes_lat_lng, schema=schema_nodes_lat_lng
    )
    return ways_df, nodes_df, nodes_lat_lng_df


class TestDangerousWaysProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("unittest-pyspark-local-testing")
            .getOrCreate()
        )
        cls.ways_df, cls.nodes_df, cls.nodes_lat_lng_df = create_mock_dataframes(
            cls.spark
        )

    def setUp(self):
        self.processor = DangerousWaysProcessor(
            self.spark, self.ways_df, self.nodes_df, self.nodes_lat_lng_df
        )

    def test_convert_bridge_ways_nodes_to_connections(self):
        bridge_way_to_node = self.processor.df_all_roads.select(
            col("way_id"), explode(col("nodes")).alias("node")
        ).filter(col("way_id") == 1)

        connections = self.processor.convert_bridge_ways_nodes_to_connections(
            bridge_way_to_node
        )
        result = connections.collect()

        self.assertEqual(len(result), 4)
        self.assertEqual(result[0]["way_id"], 1)
        self.assertEqual(result[0]["bridge_way_id"], 1)
        self.assertEqual(result[0]["ways_away"], 0)
        self.assertEqual(result[0]["parent_node_to_bridge_node_distance_meters"], 0)

    def test_process_data(self):

        # Test with default parameters
        self.processor.process_data(min_length=10000)
        result = self.processor.connections_dataset_ordered.collect()

        self.assertEqual(self.processor.connections_dataset_ordered.count(), 11)
        self.assertEqual(max([row["ways_away"] for row in result]), 3)
        self.assertEqual(min([row["ways_away"] for row in result]), 1)
        self.assertEqual(
            max([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            114546.77,
        )
        self.assertEqual(
            min([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            0.0,
        )

        # Test with max_depth=2, in this case maximum ways_away should be 2
        self.processor.process_data(min_depth=1, max_depth=2)
        result = self.processor.connections_dataset_ordered.collect()

        self.assertEqual(self.processor.connections_dataset_ordered.count(), 10)
        self.assertEqual(max([row["ways_away"] for row in result]), 2)
        self.assertEqual(min([row["ways_away"] for row in result]), 1)
        self.assertEqual(
            max([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            104297.69,
        )
        self.assertEqual(
            min([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            0,
        )

        # Test with max_depth=7 and min_length=5. Because min_length=5, the loop will stop when parent_node_to_bridge_node_distance_meters
        # is >= 5, so the max depth of 7 iterations will not be reached if the condition is met earlier.
        self.processor.process_data(min_depth=2, max_depth=7, min_length=5)
        result = self.processor.connections_dataset_ordered.collect()

        self.assertEqual(self.processor.connections_dataset_ordered.count(), 10)
        self.assertEqual(max([row["ways_away"] for row in result]), 2)
        self.assertEqual(min([row["ways_away"] for row in result]), 1)
        self.assertEqual(
            max([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            104297.69,
        )
        self.assertEqual(
            min([row["parent_node_to_bridge_node_distance_meters"] for row in result]),
            0,
        )

    def test_get_connections_nodes(self):
        self.processor.process_data()

        connections_nodes = self.processor.get_connections_nodes()
        result = connections_nodes.collect()

        self.assertEqual(len(result), 11)
        self.assertIn("node_id", result[0])
        self.assertIn("lat_degrees", result[0])
        self.assertIn("lng_degrees", result[0])


unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)
