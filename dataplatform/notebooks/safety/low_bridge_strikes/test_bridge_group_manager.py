# Databricks notebook source
# MAGIC %pip install networkx

# COMMAND ----------

import os

os.chdir("/Workspace/backend/safety/low_bridge_strikes/")

# COMMAND ----------

# MAGIC %run ./bridge_group_manager

# COMMAND ----------


import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col


class TestBridgeGroupManager(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("unittest-pyspark-local-testing")
            .getOrCreate()
        )

    def setUp(self):
        self.mock_data()
        self.bridge_group_manager = BridgeGroupManager(
            self.spark, self.df_osm_maps_spark
        )

    def test_create_bridge_groups(self):
        result = self.bridge_group_manager.create_bridge_groups().collect()

        self.assertEqual(len(result), 27)

        # Check that there are no duplicate way_ids
        way_ids = [row["way_id"] for row in result]
        self.assertEqual(len(way_ids), len(set(way_ids)))

        row_way_id_1 = next(row for row in result if row["way_id"] == 1)
        self.assertEqual(row_way_id_1["bridge_group_id"], 1)

        row_way_id_4 = next(row for row in result if row["way_id"] == 4)
        self.assertEqual(row_way_id_4["bridge_group_id"], 3)

        row_way_id_10 = next(row for row in result if row["way_id"] == 10)
        self.assertEqual(row_way_id_10["bridge_group_id"], 5)

        row_way_id_17 = next(row for row in result if row["way_id"] == 17)
        self.assertEqual(row_way_id_17["bridge_group_id"], 17)

        row_way_id_18 = next(row for row in result if row["way_id"] == 18)
        self.assertEqual(row_way_id_18["bridge_group_id"], 17)

    def mock_data(self):
        schema = StructType(
            [
                StructField("way_id", LongType(), True),
                StructField("nodes", ArrayType(LongType()), True),
                StructField("max_height", StringType(), True),
                StructField(
                    "coordinates",
                    ArrayType(
                        StructType(
                            [
                                StructField("lon", DoubleType(), True),
                                StructField("lat", DoubleType(), True),
                            ]
                        )
                    ),
                    True,
                ),
                StructField("exit_node", StringType(), True),
            ]
        )

        data = [
            Row(
                way_id=1,
                nodes=[11, 12, 13],
                max_height="3.5",
                coordinates=[
                    Row(lon=1.1, lat=1.1),
                    Row(lon=1.2, lat=1.2),
                    Row(lon=1.3, lat=1.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=2,
                nodes=[13, 14, 15, 104],
                max_height="3.5",
                coordinates=[
                    Row(lon=1.3, lat=1.3),
                    Row(lon=1.4, lat=1.4),
                    Row(lon=1.5, lat=1.5),
                    Row(lon=1.6, lat=1.6),
                ],
                exit_node=None,
            ),
            Row(
                way_id=3,
                nodes=[21, 22, 23],
                max_height="4.0",
                coordinates=[
                    Row(lon=2.1, lat=2.1),
                    Row(lon=2.2, lat=2.2),
                    Row(lon=2.3, lat=2.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=4,
                nodes=[23, 24, 101],
                max_height="4.0",
                coordinates=[
                    Row(lon=2.3, lat=2.3),
                    Row(lon=2.4, lat=2.4),
                    Row(lon=2.5, lat=2.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=5,
                nodes=[31, 32, 33],
                max_height="3.5",
                coordinates=[
                    Row(lon=3.1, lat=3.1),
                    Row(lon=3.2, lat=3.2),
                    Row(lon=3.3, lat=3.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=6,
                nodes=[33, 34, 35, 105],
                max_height="3.5",
                coordinates=[
                    Row(lon=3.3, lat=3.3),
                    Row(lon=3.4, lat=3.4),
                    Row(lon=3.5, lat=3.5),
                    Row(lon=3.6, lat=3.6),
                ],
                exit_node=None,
            ),
            Row(
                way_id=7,
                nodes=[41, 42, 43],
                max_height="5.0",
                coordinates=[
                    Row(lon=4.1, lat=4.1),
                    Row(lon=4.2, lat=4.2),
                    Row(lon=4.3, lat=4.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=8,
                nodes=[43, 44, 45],
                max_height="5.0",
                coordinates=[
                    Row(lon=4.3, lat=4.3),
                    Row(lon=4.4, lat=4.4),
                    Row(lon=4.5, lat=4.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=9,
                nodes=[51, 52, 53],
                max_height="3.5",
                coordinates=[
                    Row(lon=5.1, lat=5.1),
                    Row(lon=5.2, lat=5.2),
                    Row(lon=5.3, lat=5.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=10,
                nodes=[53, 32, 55],
                max_height="3.5",
                coordinates=[
                    Row(lon=5.3, lat=5.3),
                    Row(lon=5.4, lat=5.4),
                    Row(lon=5.5, lat=5.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=11,
                nodes=[61, 62, 63],
                max_height="4.5",
                coordinates=[
                    Row(lon=6.1, lat=6.1),
                    Row(lon=6.2, lat=6.2),
                    Row(lon=6.3, lat=6.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=12,
                nodes=[63, 64, 65],
                max_height="4.5",
                coordinates=[
                    Row(lon=6.3, lat=6.3),
                    Row(lon=6.4, lat=6.4),
                    Row(lon=6.5, lat=6.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=13,
                nodes=[71, 72, 73],
                max_height="4.5",
                coordinates=[
                    Row(lon=7.1, lat=7.1),
                    Row(lon=7.2, lat=7.2),
                    Row(lon=7.3, lat=7.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=14,
                nodes=[73, 74, 75],
                max_height="3.5",
                coordinates=[
                    Row(lon=7.3, lat=7.3),
                    Row(lon=7.4, lat=7.4),
                    Row(lon=7.5, lat=7.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=15,
                nodes=[81, 82, 83],
                max_height="5.5",
                coordinates=[
                    Row(lon=8.1, lat=8.1),
                    Row(lon=8.2, lat=8.2),
                    Row(lon=8.3, lat=8.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=16,
                nodes=[83, 84, 85],
                max_height="5.5",
                coordinates=[
                    Row(lon=8.3, lat=8.3),
                    Row(lon=8.4, lat=8.4),
                    Row(lon=8.5, lat=8.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=17,
                nodes=[91, 92, 93],
                max_height="3.5",
                coordinates=[
                    Row(lon=9.1, lat=9.1),
                    Row(lon=9.2, lat=9.2),
                    Row(lon=9.3, lat=9.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=18,
                nodes=[93, 94, 95],
                max_height="3.5",
                coordinates=[
                    Row(lon=9.3, lat=9.3),
                    Row(lon=9.4, lat=9.4),
                    Row(lon=9.5, lat=9.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=19,
                nodes=[101, 102, 103],
                max_height="4.0",
                coordinates=[
                    Row(lon=10.1, lat=10.1),
                    Row(lon=10.2, lat=10.2),
                    Row(lon=10.3, lat=10.3),
                ],
                exit_node=None,
            ),
            Row(
                way_id=20,
                nodes=[103, 104, 105],
                max_height="4.0",
                coordinates=[
                    Row(lon=10.3, lat=10.3),
                    Row(lon=10.4, lat=10.4),
                    Row(lon=10.5, lat=10.5),
                ],
                exit_node=None,
            ),
            Row(
                way_id=21,
                nodes=[13, 100, 101],
                max_height="3.5",
                coordinates=[
                    Row(lon=1.3, lat=1.3),
                    Row(lon=10.0, lat=10.0),
                    Row(lon=10.1, lat=10.1),
                ],
                exit_node=None,
            ),
            Row(
                way_id=22,
                nodes=[23, 200, 201],
                max_height=3.5,
                coordinates=[
                    Row(lon=2.3, lat=2.3),
                    Row(lon=20.0, lat=20.0),
                    Row(lon=20.1, lat=20.1),
                ],
                exit_node=None,
            ),
            Row(
                way_id=23,
                nodes=[35, 300, 301],
                max_height=None,
                coordinates=[
                    Row(lon=3.5, lat=3.5),
                    Row(lon=30.0, lat=30.0),
                    Row(lon=30.1, lat=30.1),
                ],
                exit_node=None,
            ),
            Row(
                way_id=24,
                nodes=[112, 113, 114],
                max_height=None,
                coordinates=[
                    Row(lon=10.2, lat=10.2),
                    Row(lon=10.3, lat=10.3),
                    Row(lon=10.4, lat=10.4),
                ],
                exit_node=None,
            ),
            Row(
                way_id=25,
                nodes=[202, 203, 204],
                max_height=None,
                coordinates=[
                    Row(lon=20.2, lat=20.2),
                    Row(lon=20.3, lat=20.3),
                    Row(lon=20.4, lat=20.4),
                ],
                exit_node=None,
            ),
            Row(
                way_id=26,
                nodes=[203, 205, 206],
                max_height=None,
                coordinates=[
                    Row(lon=10.2, lat=10.2),
                    Row(lon=10.3, lat=10.3),
                    Row(lon=10.4, lat=10.4),
                ],
                exit_node=None,
            ),
            Row(
                way_id=27,
                nodes=[206, 207, 208],
                max_height="6",
                coordinates=[
                    Row(lon=10.5, lat=10.5),
                    Row(lon=10.6, lat=10.5),
                    Row(lon=10.6, lat=10.6),
                ],
                exit_node=None,
            ),
        ]
        self.df_osm_maps_spark = spark.createDataFrame(data, schema=schema)


unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)
