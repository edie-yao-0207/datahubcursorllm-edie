# Databricks notebook source
# MAGIC %pip install geopy
# MAGIC %pip install shapely

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, BooleanType
import json
import shapely
import geopy.distance
from pyspark.sql import DataFrame
from shapely import (  # This import is required for shapely.geometry refs to work.
    geometry,
    wkt,
)

# COMMAND ----------

# UDF helper to flatten coordinates for usecases: Florida, Texas...etc
def load_coordinates_from_str(coordinates_str):
    if coordinates_str is None:
        return []
    res = json.loads(coordinates_str)
    return res


load_coordinates_from_str_udf = udf(
    load_coordinates_from_str, ArrayType(ArrayType(DoubleType()))
)

test_coordinates = "[[-81.4426067559361, 28.6200033856195], [-81.4426484548971, 28.6200899898117], [-81.4428474076247, 28.6203443347984]]"

expected_output = [
    [-81.4426067559361, 28.6200033856195],
    [-81.4426484548971, 28.6200899898117],
    [-81.4428474076247, 28.6203443347984],
]

assert load_coordinates_from_str(test_coordinates) == expected_output

# COMMAND ----------

# UDF helper to flatten coordinates for usecases: IOWA, NA, Connecticut...etc
def flatten_nested_coordinates(coordinates):
    if coordinates is None:
        return []
    res = []
    for sub in coordinates:
        for x in sub:
            res.append(x)
    return res


flatten_nested_coordinates_udf = udf(
    flatten_nested_coordinates, ArrayType(ArrayType(DoubleType()))
)

# unit test
test_coordinates = [
    [
        [-92.48655731629168, 41.09616949597914, 0.0],
        [-92.486515379992227, 41.09616949152803, 0.0],
    ],
    [[-92.485698508891304, 41.096170011869454, 0.0]],
]
expected_output = [
    [-92.48655731629168, 41.09616949597914, 0.0],
    [-92.486515379992227, 41.09616949152803, 0.0],
    [-92.485698508891304, 41.096170011869454, 0.0],
]
assert flatten_nested_coordinates(test_coordinates) == expected_output

# COMMAND ----------

# UDF helper to calculate the overall distance from 1st to last point in the list of coordinates, it is used for calculate the dataset coverage
def calculate_total_distance(coords):
    if len(coords) < 2:
        return 0.0
    total_distance = 0.0
    for i in range(len(coords) - 1):
        start = coords[i]
        end = coords[i + 1]
        total_distance += geopy.distance.distance(
            (start[1], start[0]), (end[1], end[0])
        ).miles
    return total_distance


calculate_total_distance_udf = udf(calculate_total_distance, DoubleType())

test_coordinates = [
    [-82.6628493842601, 27.7119799710815],
    [-82.6628592059915, 27.7129092380462],
    [-82.6628702095163, 27.7151216606038],
    [-82.6628736800277, 27.7192642745186],
    [-82.6628713178723, 27.7194220748773],
]
expected_output = 0.5124476114931775
assert calculate_total_distance(test_coordinates) == expected_output

# COMMAND ----------

# UDF helper to convert speed limit to milliknots
def convert_speed_limit_to_milliknots(limit: [str, int], unit: str) -> int:  # type: ignore
    if limit is None or unit is None:
        return None
    if isinstance(limit, str):
        if not limit.isdigit():
            return None
        limit = int(limit)
    elif not isinstance(limit, int):
        return None

    if unit == "mph":
        return int(round(int(limit) * 868.97624190065, 0))
    if unit == "kph":
        return int(round(int(limit) * 539.957269941, 0))
    return None


convert_speed_limit_to_milliknots_udf = udf(
    convert_speed_limit_to_milliknots, IntegerType()
)
assert convert_speed_limit_to_milliknots("20", "kph") == 10799
assert convert_speed_limit_to_milliknots(20, "kph") == 10799
assert convert_speed_limit_to_milliknots("20", "mph") == 17380
assert convert_speed_limit_to_milliknots(20, "mph") == 17380
assert convert_speed_limit_to_milliknots("aaaa", "mph") == None
assert convert_speed_limit_to_milliknots("", "mph") == None
assert convert_speed_limit_to_milliknots(None, "mph") == None
assert convert_speed_limit_to_milliknots("20", "") == None
assert convert_speed_limit_to_milliknots("20", None) == None

# COMMAND ----------


def find_deltas_between_mapmatched_datasets(
    dataset_old: DataFrame, dataset_new: DataFrame
) -> DataFrame:
    """
    This func is to find the delta between 2 dataframes that have the same schema but of different dates

    Steps:
    1. create an empty dataframes that we will store the resulting delta in S3
    2. create a hash table to store osm_way_id: speed_limit_milliknots as key:value for the dataset_old
    3. for every osm_way_id in dataset_new
      - if this wayid exist in the hash table, then compare the speed_limit_milliknots and if they are different, add it to the delta
      - else if this wayid does not exist in the hash table, then add it to the delta
    """
    old_data = dataset_old.select("osm_way_id", "speed_limit_milliknots").collect()
    old_data_dict = {
        row["osm_way_id"]: row["speed_limit_milliknots"] for row in old_data
    }

    def is_different(way_id, speed_limit_milliknots):
        if way_id in old_data_dict:
            return old_data_dict[way_id] != speed_limit_milliknots
        return True

    is_different_udf = udf(is_different, BooleanType())

    delta_df = dataset_new.filter(
        is_different_udf(col("osm_way_id"), col("speed_limit_milliknots"))
    )
    return delta_df


# COMMAND ----------


def save_deltas_in_combined_mapmatched_table(
    table_name_mapmatched, dataset_osm_matching_result
):
    if spark.catalog.tableExists(table_name_mapmatched):
        dataset_osm_matching_result_existing = spark.table(table_name_mapmatched)
        delta_df = find_deltas_between_mapmatched_datasets(
            dataset_osm_matching_result_existing, dataset_osm_matching_result
        )
        delta_cnt = delta_df.count()
        if delta_cnt > 0:
            delta_df.write.format("delta").mode("append").saveAsTable(
                table_name_mapmatched
            )
            print(
                f"{delta_cnt} rows changed, appended this delta to {table_name_mapmatched} table"
            )
    else:
        dataset_osm_matching_result.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(table_name_mapmatched)
        print(f"Map-matched table {table_name_mapmatched} is now written on S3")
