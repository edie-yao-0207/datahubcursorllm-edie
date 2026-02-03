# Databricks notebook source
import base64 as b64_lib
import calendar
from datetime import datetime
import json
import math

from delta.tables import *
import geojson
import geopandas
import h3
import h3.api.basic_int as h3_int
import numpy as np
import pandas as pd
import pyproj
from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    base64,
    col,
    collect_list,
    collect_set,
    count,
    explode,
    first,
    from_unixtime,
    lead,
    lit,
    size,
    struct,
    unbase64,
    window,
)
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType
import requests
import shapely

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------


@udf("string")
def lat_lng_to_h3_str_udf(lat, lng, res):
    return h3.geo_to_h3(lat, lng, res)


# COMMAND ----------


def polygon_to_h3_str(geom, res):
    h3_indices = set()
    geom = shapely.wkt.loads(geom)
    if type(geom) == shapely.geometry.polygon.Polygon:
        for (lng, lat) in geom.exterior.coords[:]:
            h3_idx = h3.geo_to_h3(lat, lng, res)
            h3_indices.add(h3_idx)
        polygon_geojson = shapely.geometry.mapping(geom)
        h3_indices.update(list(h3.polyfill_geojson(polygon_geojson, res)))
    elif type(geom) == shapely.geometry.multipolygon.MultiPolygon:
        polygons = list(geom)
        for polygon in polygons:
            for (lng, lat) in polygon.exterior.coords[:]:
                h3_idx = h3.geo_to_h3(lat, lng, res)
                h3_indices.add(h3_idx)
            polygon_geojson = shapely.geometry.mapping(polygon)
            h3_indices.update(list(h3.polyfill_geojson(polygon_geojson, res)))
    return list(h3_indices)


polygon_to_h3_str_udf = udf(
    lambda geom, res: polygon_to_h3_str(geom, res), ArrayType(StringType())
)

# COMMAND ----------

geod = pyproj.Geod(ellps="WGS84")

# Distance helpers
@udf("double")
def haversine_distance(start_lat, start_lng, end_lat, end_lng):
    _, _, dist = geod.inv(start_lng, start_lat, end_lng, end_lat)
    return dist


def get_raw_distance(start_lat, start_lng, end_lat, end_lng):
    _, _, dist = geod.inv(start_lng, start_lat, end_lng, end_lat)
    return dist


@udf("double")
def get_min_lng_for_distance(lat, lng, LONG, SHORT, bearing):
    min_lng, _, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(SHORT, LONG) * 180 / np.pi + bearing + 180,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return min_lng


@udf("double")
def get_min_lat_for_distance(lat, lng, LONG, SHORT, bearing):
    _, min_lat, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(SHORT, LONG) * 180 / np.pi + bearing + 180,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return min_lat


@udf("double")
def get_max_lng_for_distance(lat, lng, LONG, SHORT, bearing):
    max_lng, _, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(SHORT, LONG) * 180 / np.pi + bearing,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return max_lng


@udf("double")
def get_max_lat_for_distance(lat, lng, LONG, SHORT, bearing):
    _, max_lat, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(SHORT, LONG) * 180 / np.pi + bearing,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return max_lat


@udf("double")
def get_third_corner_lat_for_distance(lat, lng, LONG, SHORT, bearing):
    _, third_corner_lat, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(LONG, SHORT) * 180 / np.pi + bearing + 90,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return third_corner_lat


@udf("double")
def get_third_corner_lng_for_distance(lat, lng, LONG, SHORT, bearing):
    third_corner_lng, _, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(LONG, SHORT) * 180 / np.pi + bearing + 90,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return third_corner_lng


@udf("double")
def get_fourth_corner_lat_for_distance(lat, lng, LONG, SHORT, bearing):
    _, fourth_corner_lat, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(LONG, SHORT) * 180 / np.pi + bearing - 90,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return fourth_corner_lat


@udf("double")
def get_fourth_corner_lng_for_distance(lat, lng, LONG, SHORT, bearing):
    fourth_corner_lng, _, _ = geod.fwd(
        lng,
        lat,
        np.arctan2(LONG, SHORT) * 180 / np.pi + bearing - 90,
        np.sqrt((LONG / 2) * (LONG / 2) + (SHORT / 2) * (SHORT / 2)),
    )
    return fourth_corner_lng


@udf("double")
def calculate_bearing(first_lon, first_lat, last_lon, last_lat):
    """Returns bearing in degrees"""
    if not (first_lon and first_lat and last_lon and last_lat):
        return None
    bearing, _, _ = geod.inv(first_lon, first_lat, last_lon, last_lat)
    return bearing


@udf
def get_bounding_box_polygon(
    min_lat,
    min_lng,
    max_lat,
    max_lng,
    third_corner_lat,
    third_corner_lng,
    fourth_corner_lat,
    fourth_corner_lng,
):
    pointList = [
        [min_lng, min_lat],
        [third_corner_lng, third_corner_lat],
        [max_lng, max_lat],
        [fourth_corner_lng, fourth_corner_lat],
    ]
    return str(shapely.geometry.Polygon(pointList))


def get_bbox_length_and_height(bounding_box):
    length = bounding_box[2] - bounding_box[0]
    height = bounding_box[3] - bounding_box[1]
    return length, height


def get_bbox_area(bounding_box):
    l, h = get_bbox_length_and_height(bounding_box)
    return l * h


# COMMAND ----------


@udf("double")
def get_lat_for_dist_bearing(lat, lng, dist, bearing):
    _, ret_lat, _ = geod.fwd(lng, lat, bearing, dist)
    return ret_lat


@udf("double")
def get_lon_for_dist_bearing(lat, lng, dist, bearing):
    ret_lon, _, _ = geod.fwd(lng, lat, bearing, dist)
    return ret_lon


# COMMAND ----------

# from https://en.wikipedia.org/wiki/Street_suffix#United_States
ROAD_SUFFIXES = {
    "ALLEY",
    "ALLEE",
    "ALLY",
    "ALY",
    "ANNEX",
    "ANEX",
    "ANNX",
    "ANX",
    "ARCADE",
    "ARC",
    "AVENUE",
    "AV",
    "AVE",
    "AVEN",
    "AVENU",
    "AVN",
    "AVNUE",
    "BAYOU",
    "BAYOO",
    "BYU",
    "BEACH",
    "BCH",
    "BEND",
    "BND",
    "BLUFF",
    "BLUF",
    "BLF",
    "BLUFFS",
    "BLFS",
    "BOTTOM",
    "BOT",
    "BOTTM",
    "BTM",
    "BOULEVARD",
    "BOUL",
    "BOULV",
    "BLVD",
    "BRANCH",
    "BRNCH",
    "BR",
    "BRIDGE",
    "BRDGE",
    "BRG",
    "BROOK",
    "BRK",
    "BROOKS",
    "BRKS",
    "BURG",
    "BG",
    "BURGS",
    "BGS",
    "BYPASS",
    "BYPA",
    "BYPAS",
    "BYPS",
    "BYP",
    "CAMP",
    "CMP",
    "CP",
    "CANYON",
    "CANYN",
    "CNYN",
    "CYN",
    "CAPE",
    "CPE",
    "CAUSEWAY",
    "CAUSWA",
    "CSWY",
    "CENTER",
    "CEN",
    "CENT",
    "CENTR",
    "CENTRE",
    "CNTER",
    "CNTR",
    "CTR",
    "CENTERS",
    "CTRS",
    "CIRCLE",
    "CIRC",
    "CIRCL",
    "CRCL",
    "CRCLE",
    "CIR",
    "CIRCLE",
    "CIRS",
    "CLIFF",
    "CLF",
    "CLIFFS",
    "CLFS",
    "CLUB",
    "CLB",
    "COMMON",
    "CMN",
    "COMMONS",
    "CMNS",
    "CORNER",
    "COR",
    "CORNERS",
    "CORS",
    "COURSE",
    "CRSE",
    "COURT",
    "CT",
    "COURTS",
    "CTS",
    "COVE",
    "CV",
    "COVES",
    "CVS",
    "CREEK",
    "CRK",
    "CRESCENT",
    "CRSENT",
    "CRSNT",
    "CRES",
    "CREST",
    "CRST",
    "CROSSING",
    "CRSSNG",
    "XING",
    "CROSSROAD",
    "XRD",
    "CURVE",
    "CURV",
    "DALE",
    "DL",
    "DAM",
    "DM",
    "DIVIDE",
    "DIV",
    "DVD",
    "DV",
    "DRIVE",
    "DRIV",
    "DRV",
    "DR",
    "DRIVES",
    "DRS",
    "ESTATE",
    "EST",
    "ESTATES",
    "ESTS",
    "EXPRESSWAY",
    "EXP",
    "EXPR",
    "EXPRESS",
    "EXPW",
    "EXPY",
    "EXTENSION",
    "EXTN",
    "EXTNSN",
    "EXT",
    "EXTENSIONS",
    "EXTS",
    "FALL",
    "FALLS",
    "FLS",
    "FERRY",
    "FRRY",
    "FRY",
    "FIELD",
    "FLD",
    "FIELDS",
    "FLDS",
    "FLAT",
    "FLT",
    "FLATS",
    "FLTS",
    "FORD",
    "FRD",
    "FORDS",
    "FRDS",
    "FOREST",
    "FRST",
    "FORGE",
    "FORG",
    "FRG",
    "FORGES",
    "FRGS",
    "FORK",
    "FRK",
    "FORKS",
    "FRKS",
    "FORT",
    "FRT",
    "FT",
    "FREEWAY",
    "FREEWY",
    "FRWAY",
    "FRWY",
    "FWY",
    "GARDEN",
    "GARDN",
    "GRDEN",
    "GRDN",
    "GDN",
    "GARDENS",
    "GDNS",
    "GATEWAY",
    "GATEWY",
    "GATWAY",
    "GTWAY",
    "GTWY",
    "GLEN",
    "GLN",
    "GLENS",
    "GLNS",
    "GRN",
    "GREENS",
    "GRNS",
    "GROVE",
    "GROV",
    "GRV",
    "GROVES",
    "GRVS",
    "HARBOR",
    "HARB",
    "HARBR",
    "HRBOR",
    "HBR",
    "HARBORS",
    "HBRS",
    "HAVEN",
    "HVN",
    "HEIGHTS",
    "HTS",
    "HIGHWAY",
    "HIGHWY",
    "HIWAY",
    "HIWY",
    "HWAY",
    "HWY",
    "HILL",
    "HL",
    "HILLS",
    "HLS",
    "HOLLOW",
    "HLLW",
    "HOLW",
    "HOLWS",
    "INLET",
    "INLT",
    "ISLAND",
    "IS",
    "ISLANDS",
    "ISS",
    "ISLE",
    "JUNCTION",
    "JCTION",
    "JCTN",
    "JUNCTN",
    "JUNCTON",
    "JCT",
    "JUNCTIONS",
    "JCTS",
    "KEY",
    "KY",
    "KEYS",
    "KYS",
    "KNOLL",
    "KNOL",
    "KNL",
    "KNOLLS",
    "KNLS",
    "LAKE",
    "LK",
    "LAKES",
    "LKS",
    "LAND",
    "LANDING",
    "LNDNG",
    "LNDG",
    "LANE",
    "LN",
    "LIGHT",
    "LGT",
    "LIGHTS",
    "LGTS",
    "LOAF",
    "LF",
    "LOCK",
    "LCK",
    "LOCKS",
    "LCKS",
    "LODGE",
    "LDGE",
    "LODG",
    "LDG",
    "LOOP",
    "MALL",
    "MANOR",
    "MNR",
    "MANORS",
    "MNRS",
    "MEADOW",
    "MDW",
    "MEADOWS",
    "MEDOWS",
    "MDWS",
    "MEWS",
    "MILL",
    "ML",
    "MILLS",
    "MLS",
    "MISSION",
    "MSN",
    "MOTORWAY",
    "MTWY",
    "MOUNT",
    "MT",
    "MOUNTAIN",
    "MTN",
    "MOUNTAINS",
    "MTNS",
    "NECK",
    "NCK",
    "ORCHARD",
    "ORCHRD",
    "ORCH",
    "OVAL",
    "OVL",
    "OVERPASS",
    "OPAS",
    "PARK",
    "PRK",
    "PARKS",
    "PARK",
    "PARKWAY",
    "PARKWY",
    "PKWAY",
    "PKY",
    "PKWY",
    "PARKWAYS",
    "PKWYS",
    "PASS",
    "PASSAGE",
    "PSGE",
    "PATH",
    "PIKE",
    "PINE",
    "PNE",
    "PINES",
    "PNES",
    "PLACE",
    "PL",
    "PLAIN",
    "PLN",
    "PLAINS",
    "PLNS",
    "PLAZA",
    "PLZA",
    "PLZ",
    "POINT",
    "PT",
    "POINTS",
    "PTS",
    "PORT",
    "PRT",
    "PORTS",
    "PRTS",
    "PRAIRIE",
    "PRR",
    "PR",
    "RADIAL",
    "RAD",
    "RADIEL",
    "RADL",
    "RAMP",
    "RANCH",
    "RNCH",
    "RNCHS",
    "RAPID",
    "RPD",
    "RAPIDS",
    "RPDS",
    "REST",
    "RST",
    "RIDGE",
    "RDGE",
    "RDG",
    "RIDGES",
    "RDGS",
    "RIVER",
    "RVR",
    "RIVR",
    "RIV",
    "ROAD",
    "RD",
    "ROADS",
    "RDS",
    "ROUTE",
    "RTE",
    "ROW",
    "RUE",
    "RUN",
    "SHOAL",
    "SHL",
    "SHOALS",
    "SHLS",
    "SHORE",
    "SHR",
    "SHORES",
    "SHRS",
    "SKYWAY",
    "SKWY",
    "SPRING",
    "SPNG",
    "SPRNG",
    "SPG",
    "SPRINGS",
    "SPGS",
    "SPUR",
    "SQUARE",
    "SQR",
    "SQRE",
    "SQU",
    "SQ",
    "SQUARES",
    "SQS",
    "STATION",
    "STATN",
    "STN",
    "STA",
    "STRAVENUE",
    "STRAV",
    "STRAVEN",
    "STRAVN",
    "STRVN",
    "STRVNUE",
    "STRA",
    "STREAM",
    "STREME",
    "STRM",
    "STREET",
    "STR",
    "STRT",
    "ST",
    "STREETS",
    "STS",
    "SUMMIT",
    "SUMIT",
    "SUMITT",
    "SMT",
    "TERRACE",
    "TERR",
    "TER",
    "THROUGHWAY",
    "TRWY",
    "TRACE",
    "TRCE",
    "TRACK",
    "TRAK",
    "TRK",
    "TRKS",
    "TRAFFICWAY",
    "TRFY",
    "TRAIL",
    "TRL",
    "TRAILER",
    "TRLR",
    "TUNNEL",
    "TUNL",
    "TURNPIKE",
    "TRNPK",
    "TURNPK",
    "TPKE",
    "UNDERPASS",
    "UPAS",
    "UNION",
    "UN",
    "UNIONS",
    "UNS",
    "VALLEY",
    "VALLY",
    "VLLY",
    "VLY",
    "VALLEY",
    "VLYS",
    "VIADUCT",
    "VDCT",
    "VIADCT",
    "VIA",
    "VIEW",
    "VW",
    "VIEWS",
    "VWS",
    "VILLAGE",
    "VILL",
    "VILLAG",
    "VILLG",
    "VLG",
    "VILLAGES",
    "VLGS",
    "VILLE",
    "VL",
    "VISTA",
    "VIST",
    "VST",
    "VSTA",
    "VIS",
    "WALK",
    "WALL",
    "WAY",
    "WY",
    "WELL",
    "WL",
    "WELLS",
    "WLS",
}

# COMMAND ----------


@udf
def fix_street(raw_street):
    if not raw_street:
        return ""
    street = raw_street.upper()
    if street[0] == "0":
        street = street[1:]
    split_street = street.split(" ")
    if len(split_street) == 1:
        return street
    street_parts = []
    for part in split_street:
        if part not in ROAD_SUFFIXES:
            street_parts.append(part)
    return " ".join(street_parts)


# COMMAND ----------


def get_raw_bearing_min(bearing, lim):
    if bearing > (-180 + lim):
        return float(bearing - lim)
    return float((bearing - lim) % 180)


def get_raw_bearing_max(bearing, lim):
    if bearing < 180 - lim:
        return float(bearing + lim)
    return float((bearing + lim) % -180)


def get_bearing_within_lim(b1, b2, lim):
    b_min = get_raw_bearing_min(b1, lim)
    b_max = get_raw_bearing_max(b1, lim)
    if b1 < (-180 + lim) or b1 > 180 - lim:
        return b2 <= b_max or b2 >= b_min
    return b2 >= b_min and b2 <= b_max


# COMMAND ----------


@udf("double")
def get_bearing_min(bearing, lim):
    if bearing > (-180 + lim):
        return float(bearing - lim)
    return float((bearing - lim) % 180)


@udf("double")
def get_bearing_max(bearing, lim):
    if bearing < 180 - lim:
        return float(bearing + lim)
    return float((bearing + lim) % -180)


@udf("double")
def calculate_reverse_bearing(bearing):
    if bearing >= 0:
        return bearing - 180
    return bearing + 180


# COMMAND ----------


def write_table_with_date_partition(
    data_sdf, table_name, min_date_for_update="2017-01-01"
):
    data_sdf.write.format("delta").mode("ignore").option(
        "overwriteSchema", "True"
    ).partitionBy("date").saveAsTable(table_name)
    updates = data_sdf.filter(F.col("date") >= min_date_for_update)
    updates.write.format("delta").mode("overwrite").partitionBy("date").option(
        "replaceWhere", f"date >= '{min_date_for_update}'"
    ).saveAsTable(table_name)


def write_table_no_partition(data_sdf, table_name, mode="overwrite"):
    data_sdf.write.format("delta").mode(mode).option(
        "overwriteSchema", "True"
    ).saveAsTable(table_name)


def write_table_h3_res_3_partition(data_sdf, table_name, mode="overwrite"):
    data_sdf.write.format("delta").mode(mode).option(
        "overwriteSchema", "True"
    ).partitionBy("hex_id_res_3").saveAsTable(table_name)


# COMMAND ----------


def gen_bounding_box_and_bearings(intersections_sdf, include_reverse=True):
    LONG = 65
    SHORT = 40
    BEARING_RANGE = 45
    intersections_bbox = intersections_sdf.withColumn(
        "min_lat",
        get_min_lat_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "min_lng",
        get_min_lng_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "max_lat",
        get_max_lat_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "max_lng",
        get_max_lng_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "third_corner_lat",
        get_third_corner_lat_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "third_corner_lng",
        get_third_corner_lng_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "fourth_corner_lat",
        get_fourth_corner_lat_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "fourth_corner_lng",
        get_fourth_corner_lng_for_distance(
            col("latitude"), col("longitude"), lit(LONG), lit(SHORT), col("bearing")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "side_length",
        haversine_distance(
            col("min_lat"), col("min_lng"), col("min_lat"), col("max_lng")
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "bearing_min", get_bearing_min(col("bearing"), lit(BEARING_RANGE))
    )
    intersections_bbox = intersections_bbox.withColumn(
        "bearing_max", get_bearing_max(col("bearing"), lit(BEARING_RANGE))
    )
    if include_reverse:
        intersections_bbox = intersections_bbox.withColumn(
            "reverse_bearing_min",
            get_bearing_min(col("reverse_bearing"), lit(BEARING_RANGE)),
        )
        intersections_bbox = intersections_bbox.withColumn(
            "reverse_bearing_max",
            get_bearing_max(col("reverse_bearing"), lit(BEARING_RANGE)),
        )
    intersections_bbox = intersections_bbox.withColumn(
        "bounding_box_poly",
        get_bounding_box_polygon(
            col("min_lat"),
            col("min_lng"),
            col("max_lat"),
            col("max_lng"),
            col("third_corner_lat"),
            col("third_corner_lng"),
            col("fourth_corner_lat"),
            col("fourth_corner_lng"),
        ),
    )
    intersections_bbox = intersections_bbox.withColumn(
        "bounding_box_h3", polygon_to_h3_str_udf(col("bounding_box_poly"), lit(13))
    )
    return intersections_bbox


# COMMAND ----------


@udf("boolean")
def bearing_in_range(
    intsec_bearing, loc_bearing, intsec_bearing_max, intsec_bearing_min, lim=30
):
    if intsec_bearing < -180 + lim or intsec_bearing > 180 - lim:
        return loc_bearing <= intsec_bearing_max or loc_bearing >= intsec_bearing_min
    return loc_bearing >= intsec_bearing_min and loc_bearing <= intsec_bearing_max


# COMMAND ----------


def map_s3_path_to_dbfs(s3path):
    """
    maps s3://.../... to mounted path.
    to use with non-spark functions, prepend "/dbfs/mnt/"
    """
    return s3path.replace("s3://", "/dbfs/mnt/")


def s3url_exists(s3url):
    """
    s3urls_exists checks for the existence of an s3 object
    among the s3 buckets mounted to the executing databricks cluster
    and returns True if the given s3url exists
    :param s3urls: a well-formatted s3url (s3://bucket/key)
    """
    return os.path.exists(s3url)


@udf("boolean")
def s3url_exists_udf(dbfs_path):
    return os.path.exists(dbfs_path)


# COMMAND ----------


def table_exists(table_name):
    try:
        spark.table(table_name)
        return True
    except:
        return False


def write_h3_table(table_name, indices):
    schema = StructType([StructField("h3_index", StringType(), True)])
    indices_for_df = [[index] for index in indices]  # spark requires list of lists
    df = spark.createDataFrame(indices_for_df, schema)
    df.write.mode("overwrite").saveAsTable(table_name)


def drop_h3_table(table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# COMMAND ----------

DEFAULT_CONFIG = {
    "new_cluster": {
        "num_workers": None,
        "autoscale": {"min_workers": 2, "max_workers": 8},
        "cluster_name": "",
        "spark_version": "7.3.x-scala2.12",
        "spark_conf": {
            "viewMaterializationDataset": "spark_view_materialization",
            "spark.databricks.delta.stalenessLimit": "15m",
            "spark.hadoop.fs.s3.impl": "com.databricks.s3a.S3AFileSystem",
            "spark.sql.extensions": "com.samsara.dataplatform.sparkrules.Extension",
            "spark.databricks.delta.properties.defaults.logRetentionDuration": "INTERVAL 30 DAYS",
            "spark.hadoop.fs.s3a.acl.default": "BucketOwnerFullControl",
            "spark.hadoop.aws.glue.max-error-retries": "10",
            "spark.databricks.hive.metastore.glueCatalog.enabled": True,
            "spark.hadoop.fs.s3n.impl": "com.databricks.s3a.S3AFileSystem",
            "spark.hadoop.fs.gs.project.id": "samsara-data",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64MB",
            "spark.driver.maxResultSize": "8000000000",
            "spark.hadoop.google.cloud.auth.service.account.enable": True,
            "spark.hadoop.fs.s3a.canned.acl": "BucketOwnerFullControl",
            "spark.databricks.delta.history.metricsEnabled": True,
            "spark.databricks.delta.autoCompact.enabled": True,
            "temporaryGcsBucket": "samsara-bigquery-spark-connector",
            "spark.databricks.service.server.enabled": True,
            "spark.databricks.repl.allowedLanguages": "sql,python",
            "spark.sql.warehouse.dir": "s3://samsara-databricks-playground/warehouse/",
            "spark.databricks.delta.properties.defaults.checkpointRetentionDuration": "INTERVAL 30 DAYS",
            "viewsEnabled": True,
            "spark.databricks.cluster.profile": "serverless",
            "spark.hadoop.fs.s3a.impl": "com.databricks.s3a.S3AFileSystem",
            "credentialsFile": "/databricks/samsara-data-5142c7cd3ba2.json",
            "spark.sql.sources.default": "delta",
            "spark.hadoop.aws.glue.partition.num.segments": "1",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
            "spark.databricks.delta.optimizeWrite.enabled": True,
            "spark.databricks.hive.metastore.client.pool.size": "3",
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile": "/databricks/samsara-data-5142c7cd3ba2.json",
        },
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-west-2a",
            "instance_profile_arn": "arn:aws:iam::492164655156:instance-profile/dev-cluster",
            "spot_bid_price_percent": 100,
            "ebs_volume_type": None,
            "ebs_volume_count": None,
            "ebs_volume_size": None,
        },
        "node_type_id": "rd-fleet.4xlarge",
        "ssh_public_keys": [],
        "custom_tags": {
            "ResourceClass": "Serverless",
            "samsara:team": "datascience",
            "samsara:service": "databricksjobcluster-datascience",
            "samsara:product-group": "datascience",
            "samsara:rnd-allocation": 1,
            "samsara:pooled-job:is-production-job": True,
        },
        "cluster_log_conf": {
            "s3": {
                "destination": "s3://samsara-databricks-cluster-logs/datascience",
                "region": "us-west-2",
                "enable_encryption": True,
                "canned_acl": "bucket-owner-full-control",
            }
        },
        "spark_env_vars": {},
        "enable_elastic_disk": True,
        "cluster_source": "JOB",
        "init_scripts": [],
        "policy_id": "E06010C9DA000038",
    },
    "max_retries": 5,
    "email_notifications": {
        "on_start": [],
        "on_success": [],
        "on_failure": ["datascience@samsara.com"],
    },
    "libraries": [
        {"pypi": {"package": "bokeh==2.1.1"}},
        {"pypi": {"package": "jinja2==2.11.2"}},
        {"pypi": {"package": "geopandas==0.8.1"}},
        {"pypi": {"package": "h3==3.6.4"}},
        {"pypi": {"package": "python-snappy==0.7.3"}},
        {"pypi": {"package": "geojson==2.5.0"}},
        {"pypi": {"package": "contextlib2"}},
        {"pypi": {"package": "scikit-image"}},
    ],
}


# COMMAND ----------


def get_cluster_config(job_config, cluster_config_type):
    cluster_config = job_config["new_cluster"]
    if cluster_config_type != "default":
        cluster_config["node_type_id"] = "g4dn.8xlarge"
    if cluster_config_type == "telemetry_inference":
        cluster_config["spark_version"] = "7.1.x-gpu-ml-scala2.12"
    if cluster_config_type == "trip_still_inference":
        cluster_config["spark_version"] = "6.6.x-gpu-ml-scala2.11"
    return cluster_config


def get_notebook_task(nb_path, region_name, additional_nb_params):
    base_params = additional_nb_params
    base_params["region_name"] = region_name
    nb_task = {"notebook_path": nb_path, "base_parameters": base_params}
    return nb_task


def trigger_next_nb(
    nb_path,
    job_name,
    region_name,
    cluster_config_type="default",
    additional_nb_params={},
):
    job_config = DEFAULT_CONFIG
    job_config["new_cluster"] = get_cluster_config(job_config, cluster_config_type)
    job_config["run_name"] = job_name
    job_config["notebook_task"] = get_notebook_task(
        nb_path, region_name, additional_nb_params
    )

    api_url = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .apiUrl()
        .getOrElse(None)
    )
    api_token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .apiToken()
        .getOrElse(None)
    )

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-type": "application/json",
    }
    r = requests.post(
        api_url + "/api/2.0/jobs/runs/submit", headers=headers, json=job_config
    )
    print("API RESPONSE")
    print(r.text)


# COMMAND ----------


def get_steps_widgets():
    all_steps = [
        "trip_still_prep",
        "telem_gen_intersections",
        "telem_gen_location_data",
        "telem_partition_telem_data",
        "telem_histograms_gen",
        "telem_inference",
        "trip_still_locations",
        "trip_stills_intersections",
        "trip_stills_gen_image_paths",
        "trip_stills_inference",
        "trip_still_inference_processor",
        "combine_data",
        "combine_all",
    ]
    widget_vals = {}
    for step in all_steps:
        try:
            step_mode = dbutils.widgets.get(f"{step}_mode")
            widget_vals[f"{step}_mode"] = step_mode
        except Exception:
            pass
        try:
            step_min_date = dbutils.widgets.get(f"{step}_min_date")
            widget_vals[f"{step}_min_date"] = step_min_date
        except Exception:
            pass
    return widget_vals


def get_step_value(step, step_values):
    mode_val = "overwrite"
    if f"{step}_mode" in step_values:
        mode_val = step_values[f"{step}_mode"]
    min_date_val = "2017-01-01"
    if f"{step}_min_date" in step_values:
        min_date_val = step_values[f"{step}_min_date"]
    return mode_val, min_date_val
