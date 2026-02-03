# Databricks notebook source

# COMMAND ----------

from collections import namedtuple
import json

from pyspark.sql.types import IntegerType, MapType

# COMMAND ----------


def get_max_passenger_speed_limit(restrictions_arr):
    # multinet/documentation/mn/specifications/mn_3-6-13-1_00_final/specifications/mn_data_spec/theme_roads_and_ferries/speed_restriction_information/speed_restriction_information.html?hl=speed%2Crestrictions
    # 0 : All Vehicle Types; 11 : Passenger Cars; 12 : Residential Vehicle (Motorhomes); 17: public bus
    # Do not include motorhomes since we don't need to support that and public bus since we'll include that the commercial
    # speed limit map
    restrictions_arr = filter(
        lambda r: r.vehicle_type == 0 or r.vehicle_type == 11, restrictions_arr
    )

    # For now, ignore time-based restrictions
    restrictions_arr = list(filter(lambda r: r.timedom is None, restrictions_arr))

    if len(restrictions_arr) == 0:
        return None

    # Return the speedlimit associated with the highest speed limit.
    restrictions_sorted = sorted(
        restrictions_arr, key=lambda r: r.maxspeed, reverse=True
    )
    return restrictions_sorted[0].maxspeed


max_passenger_speed_udf = udf(get_max_passenger_speed_limit, IntegerType())

# COMMAND ----------


# Convert json to named tuple so that dot.notation works when accessing the object. In DBX the restrictions array is represented as an object, not a dict.
assert (
    get_max_passenger_speed_limit(
        json.loads(
            '[{"seqnum": 2, "maxspeed": 45, "vehicle_type": 0, "subseqnum": null, "timedom": null, "val_dir": 2}, {"seqnum": 1, "maxspeed": 55, "vehicle_type": 0, "subseqnum": null, "timedom": null, "val_dir": 3}]',
            object_hook=lambda d: namedtuple("restrictions", d.keys())(*d.values()),
        )
    )
    == 55
)
