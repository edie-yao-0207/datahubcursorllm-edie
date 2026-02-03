# Databricks notebook source
# MAGIC %pip install shapely
# MAGIC %pip install h3
# MAGIC %pip install geopandas
# MAGIC %pip install geodatasets

# COMMAND ----------

import os

os.chdir("/Workspace/backend/safety/low_bridge_strikes/")

# COMMAND ----------

# MAGIC %run ./udf_helpers

# COMMAND ----------

# MAGIC %run ./here_maps_processor

# COMMAND ----------

import os

attributes_data_FC2 = """
,HEIGHT_RESTRICTION,LINK_IDS
510,400,863125368
512,400,870371628
514,470,910582111
515,470,910582112
516,470,990800970
519,470,990800971
520,480,1145652486
521,400,1150043854
524,400,1150043855
525,480,1182702112
526,480,1182702117
527,480,1182702119
528,480,1182702120
529,480,1182702128
530,480,1182702129
531,470,1201818858
533,470,1201818859
535,480,1224583360
536,480,1224583361
537,480,1249184113
538,480,1249184114

"""
filename = "/dbfs/test_data/attributes_data_FC2.csv"
os.makedirs(os.path.dirname(filename), exist_ok=True)

with open(filename, "w") as csvfile:
    csvfile.write(attributes_data_FC2)

road_geometry_data_FC2 = """
,LINK_ID,LONG_HAUL,NAME,NAMES,TUNNEL,BRIDGE,LAT,LON,ZLEVEL,ELEVATION,TOPOLOGY_ID,START_OFFSET,END_OFFSET
1830,1126519046,Y,A406 / North Circular Road,,N,N,"5160016,6,8,6,3,6,2,6,1","-176,-34,-44,-47,-25,-50,-21,-70,-28","-1,,-1,,,,,,-1",",,,,,,,,",143968167,046502,070815
1831,1131691186,Y,A406 / North Circular Road,,N,N,"5160598,13","-2613,-29",",",",",864559223,50701,55931
1832,1144454772,Y,A406 / North Circular Road,,N,N,"5161256,6","-11135,-77",",",",",140293408,069319,081959
1833,1144454773,Y,A406 / North Circular Road,,N,N,"5161262,2","-11212,-38",",",",",140293408,063107,069319
1834,1144457588,Y,A406 / North Circular Road,,N,N,"5161267,","-11363,44",",",",",140293408,44672,051839
2009,1201818858,Y,Brunswick Road,,N,N,"5151263,5,8,5,4","-816,1,3,3,6",",,,,",",,,,",193016357,00,1
2010,1201818859,Y,Brunswick Road,,N,N,"5151272,7,5,3,,-2","-773,-3,-4,-8,-8,-7",",,,,,",",,,,,",632377148,78644,1
2011,1205419325,Y,A12 / East Cross Route,,N,N,"5153653,15,32,17,17,16","-2543,-5,-9,-4,-3,-2",",,,,,",",,,,,",171498423,20159,38499
2012,1205419326,Y,A12 / East Cross Route,,N,N,"5153750,32,41,62,13","-2566,-4,-9,-15,-4",",,1,,",",,,,",171498423,38499,66421
2013,1205419547,Y,A12 / Blackwall Tunnel Northern Approach,,N,N,"5153351,25,6","-2164,-45,-10",",,",",,",144119933,048180,057562
2014,1205419548,Y,A12 / Blackwall Tunnel Northern Approach,,N,N,"5153382,66,9,8,7,7","-2219,-122,-16,-13,-11,-11",",,,,,",",,,,,",144119933,018731,048180
2015,1216522660,Y,A406 / North Circular Road,,N,N,"5161418,9","-5999,-63",",",",",189421412,80393,1
2016,1218087109,Y,A406 / North Circular Road,,N,N,"5161423,18","-12586,-48",",",",",100105929,0,52382
2017,1218087110,Y,A406 / North Circular Road,,N,N,"5161441,17","-12634,-43",",",",",100105929,52382,1
2018,1224583360,Y,A406 / North Circular Road,,N,Y,"5160735,11","-14850,36",",",",",164300034,0,3458
2019,1224583361,Y,A406 / North Circular Road,,Y,N,"5160746,25","-14814,79",",",",",164300034,3458,11101
2020,1226036896,N,A10 / Great Cambridge Road,,N,N,"5164833,33","-6034,2",",",",",212295685,021653,042670
2021,1226036897,N,A10 / Great Cambridge Road,,N,N,"5164866,34","-6032,2",",",",",212295685,00,021653
2022,1226135980,N,A10 / Great Cambridge Road,,N,N,"5164742,66,58","-6018,6,4",",,",",,",82102038,0,76087
2023,1226135981,N,A10 / Great Cambridge Road,,N,N,"5164866,39","-6008,2",",",",",82102038,76087,1
2024,1226136267,N,A10 / Great Cambridge Road,,N,N,"5164743,69","-6040,5",",",",",212295685,056041,1
2025,1226136268,N,A10 / Great Cambridge Road,,N,N,"5164812,21","-6035,1",",",",",212295685,042670,056041

"""
filename = "/dbfs/test_data/road_geometry_data_FC2.csv"
os.makedirs(os.path.dirname(filename), exist_ok=True)

with open(filename, "w") as csvfile:
    csvfile.write(road_geometry_data_FC2)

# COMMAND ----------

import unittest
import pandas as pd
from unittest.mock import patch, MagicMock


class TestHereMapAttributes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.api_key = "fake_api_key"
        cls.params = {"region": "Europe"}

    def setUp(self):
        with patch.object(HereMapAttributes, "__post_init__", lambda x: None):
            self.here_map_attributes = HereMapAttributes(
                api_key=self.api_key, **self.params
            )
            self.here_map_attributes.read_test_attributes = (
                mock_read_test_attributes.__get__(self.here_map_attributes)
            )
            self.here_map_attributes.read_test_road_geometry = (
                mock_read_test_road_geometry.__get__(self.here_map_attributes)
            )

    def test_calculate_tiles_for_region(self):
        region = "Europe"
        tiles = self.here_map_attributes.calculate_tiles_for_region(region, levels=[6])
        self.assertEqual(len(tiles[6]), 672)

        region = "United States of America"
        tiles = self.here_map_attributes.calculate_tiles_for_region(region, levels=[6])
        self.assertEqual(len(tiles[6]), 208)

        region = "Canada"
        tiles = self.here_map_attributes.calculate_tiles_for_region(region, levels=[6])
        self.assertEqual(len(tiles[6]), 328)

        region = "Mexico"
        tiles = self.here_map_attributes.calculate_tiles_for_region(region, levels=[6])
        self.assertEqual(len(tiles[6]), 45)

    def test_here_maps_processor(self):
        location_data = self.here_map_attributes.read_test_road_geometry()
        attributes_data = self.here_map_attributes.read_test_attributes()
        merged_data = self.here_map_attributes.merge_location_and_attributes(
            location_data, attributes_data
        )
        filtered_data = self.here_map_attributes.filter_data(merged_data)

        self.assertFalse(filtered_data.empty)
        self.assertEqual(len(filtered_data), 2)
        self.assertEqual(filtered_data.iloc[0]["HEIGHT_RESTRICTION"], 4.8)
        self.assertEqual(filtered_data.iloc[0]["LAT"], [51.60735, 51.60746])
        self.assertEqual(filtered_data.iloc[0]["LON"], [-0.1485, -0.14814])
        self.assertEqual(filtered_data.iloc[0]["BRIDGE"], "Y")
        self.assertEqual(filtered_data.iloc[0]["TUNNEL"], "N")


def mock_read_test_attributes(self):
    attributes = {"FC2": pd.read_csv("/dbfs//test_data/attributes_data_FC2.csv")}
    attributes["FC2"]["HEIGHT_RESTRICTION"] = attributes["FC2"][
        "HEIGHT_RESTRICTION"
    ].astype(str)
    return attributes


def mock_read_test_road_geometry(self):
    geometry = {"FC2": pd.read_csv("/dbfs/test_data/road_geometry_data_FC2.csv")}
    return geometry


unittest.main(argv=["first-arg-is-ignored"], exit=False, verbosity=2)
