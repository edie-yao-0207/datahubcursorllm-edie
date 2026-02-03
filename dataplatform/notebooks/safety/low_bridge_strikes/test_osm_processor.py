# Databricks notebook source
# MAGIC %pip install pytest
# MAGIC %pip install overpy
# MAGIC %pip install h3
# MAGIC %pip install shapely
# MAGIC %pip install geopandas

# COMMAND ----------

import os

os.chdir("/Workspace/backend/safety/low_bridge_strikes/")

# COMMAND ----------

# MAGIC %run ./udf_helpers

# COMMAND ----------

# MAGIC %run ./osm_processor

# COMMAND ----------

import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    ArrayType,
    MapType,
    StringType,
    IntegerType,
    DoubleType,
)


def create_mock_osm_data_df(spark):
    data = [
        (
            1001,
            [
                {"value": "discouraged", "key": "bicycle"},
                {"value": "no", "key": "cycleway"},
                {"value": "discouraged", "key": "foot"},
                {"value": "primary", "key": "highway"},
                {"value": "2", "key": "maxheight"},
                {"value": "20 mph", "key": "maxspeed"},
                {"value": "2", "key": "maxwidth"},
                {"value": "Rotherhithe Tunnel", "key": "name"},
                {"value": "yes", "key": "tunnel"},
            ],
            [
                {"index": 0, "nodeId": 12850532},
                {"index": 1, "nodeId": 12851205},
                {"index": 2, "nodeId": 1530393769},
                {"index": 3, "nodeId": 1530393771},
                {"index": 4, "nodeId": 361692432},
                {"index": 5, "nodeId": 12851250},
                {"index": 6, "nodeId": 361692436},
                {"index": 7, "nodeId": 207562611},
                {"index": 8, "nodeId": 361692441},
                {"index": 9, "nodeId": 361692444},
                {"index": 10, "nodeId": 361692447},
                {"index": 11, "nodeId": 27496502},
                {"index": 12, "nodeId": 1969885768},
                {"index": 13, "nodeId": 1969885779},
                {"index": 14, "nodeId": 12851285},
                {"index": 15, "nodeId": 1505636036},
                {"index": 16, "nodeId": 11393804060},
                {"index": 17, "nodeId": 11393804061},
                {"index": 18, "nodeId": 361692021},
                {"index": 19, "nodeId": 207562154},
                {"index": 20, "nodeId": 1505635974},
                {"index": 21, "nodeId": 48333843},
                {"index": 22, "nodeId": 1530393765},
                {"index": 23, "nodeId": 361692064},
                {"index": 24, "nodeId": 1530393767},
                {"index": 25, "nodeId": 207562153},
                {"index": 26, "nodeId": 361691802},
                {"index": 27, "nodeId": 1505640134},
                {"index": 28, "nodeId": 48333839},
                {"index": 29, "nodeId": 11393804059},
                {"index": 30, "nodeId": 361691793},
                {"index": 31, "nodeId": 1505635998},
                {"index": 32, "nodeId": 257453688},
                {"index": 33, "nodeId": 27496479},
                {"index": 34, "nodeId": 59338636},
            ],
            [
                {"ind": 16, "latitude": 51.5085488, "longitude": -0.048379000000000005},
                {"ind": 12, "latitude": 51.5043876, "longitude": -0.048770100000000004},
                {
                    "ind": 25,
                    "latitude": 51.509829200000006,
                    "longitude": -0.047042600000000004,
                },
                {"ind": 7, "latitude": 51.501819700000006, "longitude": -0.0497645},
                {"ind": 4, "latitude": 51.5016553, "longitude": -0.0498919},
                {"ind": 21, "latitude": 51.5092287, "longitude": -0.047841400000000006},
                {"ind": 9, "latitude": 51.504161100000005, "longitude": -0.0488019},
                {"ind": 30, "latitude": 51.5102557, "longitude": -0.045749000000000005},
                {"ind": 19, "latitude": 51.5087446, "longitude": -0.048235600000000003},
                {"ind": 34, "latitude": 51.5107608, "longitude": -0.041854},
                {"ind": 6, "latitude": 51.5017671, "longitude": -0.049795400000000004},
                {
                    "ind": 3,
                    "latitude": 51.501574600000005,
                    "longitude": -0.049997900000000005,
                },
                {"ind": 18, "latitude": 51.508637500000006, "longitude": -0.0483221},
                {"ind": 26, "latitude": 51.509961700000005, "longitude": -0.0467711},
                {"ind": 32, "latitude": 51.5103828, "longitude": -0.0448362},
                {"ind": 10, "latitude": 51.5042077, "longitude": -0.0487898},
                {"ind": 28, "latitude": 51.510128300000005, "longitude": -0.0463085},
                {
                    "ind": 5,
                    "latitude": 51.501713300000006,
                    "longitude": -0.049834300000000005,
                },
                {"ind": 24, "latitude": 51.5096926, "longitude": -0.0472843},
                {"ind": 17, "latitude": 51.5085843, "longitude": -0.048360200000000006},
                {"ind": 11, "latitude": 51.5042574, "longitude": -0.0487804},
                {"ind": 33, "latitude": 51.5104526, "longitude": -0.0443138},
                {
                    "ind": 15,
                    "latitude": 51.508517100000006,
                    "longitude": -0.048388600000000004,
                },
                {"ind": 8, "latitude": 51.504102200000005, "longitude": -0.0488233},
                {"ind": 31, "latitude": 51.510315500000004, "longitude": -0.0453486},
                {
                    "ind": 1,
                    "latitude": 51.501399500000005,
                    "longitude": -0.050303600000000004,
                },
                {"ind": 0, "latitude": 51.5007248, "longitude": -0.051566},
                {"ind": 29, "latitude": 51.5101956, "longitude": -0.0460508},
                {"ind": 13, "latitude": 51.5061304, "longitude": -0.048616400000000004},
                {
                    "ind": 22,
                    "latitude": 51.509404100000005,
                    "longitude": -0.047665700000000005,
                },
                {"ind": 20, "latitude": 51.5089819, "longitude": -0.0480439},
                {"ind": 27, "latitude": 51.510040200000006, "longitude": -0.0465708},
                {"ind": 23, "latitude": 51.5095517, "longitude": -0.0474795},
                {"ind": 14, "latitude": 51.5084674, "longitude": -0.0483969},
                {"ind": 2, "latitude": 51.501492000000006, "longitude": -0.0501346},
            ],
            "20 mph",
            "primary",
        ),
        (
            1002,
            [
                {"value": "yes", "key": "cutting"},
                {"value": "no", "key": "cycleway:both"},
                {"value": "primary", "key": "highway"},
                {"value": "2", "key": "maxheight"},
                {"value": "20 mph", "key": "maxspeed"},
                {"value": "2", "key": "maxwidth"},
                {"value": "both", "key": "sidewalk"},
                {"value": "asphalt", "key": "surface"},
            ],
            [
                {"index": 0, "nodeId": 1505619008},
                {"index": 1, "nodeId": 12850532},
            ],
            [
                {"ind": 0, "latitude": 51.500673000000006, "longitude": -0.0516657},
                {"ind": 1, "latitude": 51.5007248, "longitude": -0.051566},
            ],
            "20 mph",
            "primary",
        ),
        (
            1003,
            [
                {"value": "yes", "key": "bridge"},
                {"value": "no", "key": "cycleway:both"},
                {"value": "primary", "key": "highway"},
                {"value": "2", "key": "maxheight"},
                {"value": "20 mph", "key": "maxspeed"},
                {"value": "2", "key": "maxwidth"},
                {"value": "both", "key": "sidewalk"},
                {"value": "asphalt", "key": "surface"},
            ],
            [
                {"index": 0, "nodeId": 1505619075},
                {"index": 1, "nodeId": 1505619008},
            ],
            [
                {"ind": 1, "latitude": 51.500673000000006, "longitude": -0.0516657},
                {"ind": 0, "latitude": 51.500582, "longitude": -0.051846},
            ],
            "20 mph",
            "primary",
        ),
        (
            1004,
            [{"key": "highway", "value": "primary"}, {"key": "foot", "value": "no"}],
            [
                {"index": 0, "nodeId": 11111110},
                {"index": 1, "nodeId": 11111111},
            ],
            [
                {"ind": 0, "latitude": 51.5001, "longitude": -0.0511},
                {"ind": 1, "latitude": 51.5002, "longitude": -0.0512},
            ],
            "20 mph",
            "primary",
        ),
        (
            1005,
            [{"key": "highway", "value": "primary"}, {"key": "foot", "value": "yes"}],
            [
                {"index": 0, "nodeId": 11111120},
                {"index": 1, "nodeId": 11111121},
            ],
            [
                {"ind": 0, "latitude": 51.5003, "longitude": -0.0513},
                {"ind": 1, "latitude": 51.5004, "longitude": -0.0514},
            ],
            "50 mph",
            "primary",
        ),
        (
            1006,
            [
                {"key": "highway", "value": "path"},
            ],
            [
                {"index": 0, "nodeId": 11111130},
                {"index": 1, "nodeId": 11111131},
            ],
            [
                {"ind": 0, "latitude": 51.5005, "longitude": -0.0515},
                {"ind": 1, "latitude": 51.5006, "longitude": -0.0516},
            ],
            "40 mph",
            "path",
        ),
        (
            1007,
            [{"key": "highway", "value": "primary"}, {"key": "foot", "value": "no"}],
            [
                {"index": 0, "nodeId": 11111110},
                {"index": 1, "nodeId": 11111141},
            ],
            [
                {"ind": 0, "latitude": 51.5007, "longitude": -0.0541},
                {"ind": 1, "latitude": 51.5008, "longitude": -0.0512},
            ],
            "20 mph",
            "primary",
        ),
    ]

    schema = StructType(
        [
            StructField("way_id", LongType(), True),
            StructField("tags", ArrayType(MapType(StringType(), StringType())), True),
            StructField(
                "nodes",
                ArrayType(
                    StructType(
                        [
                            StructField("index", IntegerType(), True),
                            StructField("nodeId", LongType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField(
                "nodes_lat_lng",
                ArrayType(
                    StructType(
                        [
                            StructField("ind", IntegerType(), True),
                            StructField("latitude", DoubleType(), True),
                            StructField("longitude", DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
            StructField("maxspeed", StringType(), True),
            StructField("highway", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def mock_read_data(self):
    self.df_osm_roads = create_mock_osm_data_df(self.spark)
    external_data = [(1005, "4.7m"), (1007, "default"), (1003, "3.2m")]
    columns = ["matched_way_id", "max_height"]
    self.external_bridge_data = self.spark.createDataFrame(external_data, columns)


class TestOpenStreetMapDataProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("unittest-pyspark-local-testing")
            .getOrCreate()
        )

    def setUp(self):
        self.params = {"test": True, "osm_table_name": "osm_roads"}
        with patch.object(OpenStreetMapDataProcessor, "__post_init__", lambda x: None):
            self.osm_processor = OpenStreetMapDataProcessor(self.spark, self.params)
            self.osm_processor.read_data = mock_read_data.__get__(self.osm_processor)

    def test_filter_unwanted_highways(self):
        """
        Verifies that rows are filtered out if 'highway' is in unwanted_highways,
        or if they match filter_out_tags entries.
        """
        self.osm_processor.read_data()
        self.osm_processor.filter_unwanted_highways()

        result = self.osm_processor.df_osm_maps_spark.collect()
        remaining_way_ids = [row.way_id for row in result]

        # Explanation of expected filtering:
        # 1006 has highway: path => should be removed (unwanted).
        expected_remaining_ids = [
            1001,
            1002,
            1003,
            1004,
            1005,
            1007,
        ]
        self.assertEqual(sorted(remaining_way_ids), sorted(expected_remaining_ids))

    def test_extract_max_height(self):
        with patch.object(OpenStreetMapDataProcessor, "read_data", mock_read_data):
            self.osm_processor.read_data()
            self.osm_processor.filter_unwanted_highways()
            self.osm_processor.extract_and_sort_lat_lon()
            self.osm_processor.extract_points()
            self.osm_processor.extract_locale()
            self.osm_processor.extract_max_height()

            result = self.osm_processor.df_osm_maps_spark.collect()
            self.assertEqual(len(result), 6)
            self.assertEqual(result[0]["max_height"], "2.0")
            row_1007 = next((r for r in result if r.way_id == 1007), None)
            self.assertEqual(row_1007["max_height"], None)

    def test_fill_missing_max_height_from_external(self):
        with patch.object(OpenStreetMapDataProcessor, "read_data", mock_read_data):
            self.osm_processor.read_data()
            self.osm_processor.df_osm_maps_spark = self.osm_processor.df_osm_roads
            self.osm_processor.extract_and_sort_lat_lon()
            self.osm_processor.extract_points()
            self.osm_processor.extract_locale()
            self.osm_processor.extract_max_height()
            self.osm_processor.fill_missing_max_height_from_external()

            # way_id=1007 has no maxheight in OSM but is "default" in external data.
            # way_id=1005 is "4.7m" in external data.
            # way_id=1003 already has a maxheight in OSM (should not be overwritten).

            result = self.osm_processor.df_osm_maps_spark.collect()

            # way_id=1007: previously no maxheight, should now be 5 if locale=UK and external was "default"
            row_1007 = next(r for r in result if r.way_id == 1007)
            self.assertEqual(row_1007["max_height"], "5")

            # way_id=1005: previously no maxheight in OSM, should now be 4.7 from external
            row_1005 = next(r for r in result if r.way_id == 1005)
            self.assertEqual(row_1005["max_height"], "4.7")

            # way_id=1003: already has a maxheight (extracted from tags: "2"), so it should remain unchanged
            row_1003 = next(r for r in result if r.way_id == 1003)
            self.assertEqual(row_1003["max_height"], "2.0")

    def test_extract_special_tags(self):
        with patch.object(OpenStreetMapDataProcessor, "read_data", mock_read_data):
            self.osm_processor.read_data()
            self.osm_processor.filter_unwanted_highways()
            self.osm_processor.extract_special_tags()

            result = self.osm_processor.df_osm_maps_spark.collect()
            self.assertEqual(len(result), 6)
            self.assertEqual(result[0]["tags"], ["maxheight", "tunnel"])

    def test_extract_and_sort_lat_lon(self):
        with patch.object(OpenStreetMapDataProcessor, "read_data", mock_read_data):
            self.osm_processor.read_data()
            self.osm_processor.filter_unwanted_highways()
            self.osm_processor.extract_and_sort_lat_lon()
            self.osm_processor.extract_points()
            self.osm_processor.extract_locale()

            result = self.osm_processor.df_osm_maps_spark.collect()
            self.assertEqual(len(result), 6)
            self.assertEqual(result[1]["lat_degrees"], [51.500673000000006, 51.5007248])
            self.assertEqual(result[1]["lng_degrees"], [-0.0516657, -0.051566])

    def test_create_nodes_dataframe(self):
        with patch.object(OpenStreetMapDataProcessor, "read_data", mock_read_data):
            self.osm_processor.read_data()
            self.osm_processor.create_nodes_dataframe()

            result = self.osm_processor.df_nodes.collect()
            self.assertEqual(len(result), 42)
            self.assertEqual(result[0]["node_id"], 1530393769)


unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)
