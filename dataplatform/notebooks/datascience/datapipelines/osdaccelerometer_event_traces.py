import copy
import random
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import Row
from pyspark.sql import Window as W
from pyspark.sql import functions as F
from pyspark.sql import types as T


def sort_values(collection):
    if isinstance(collection, (list, tuple)):
        return sorted(collection)
    elif isinstance(collection, dict):
        return [collection[str(idx)] for idx in range(1, len(collection) + 1)]
    else:
        raise TypeError


def merge_dicts(dicts):
    return reduce(lambda x, y: {**x, **y}, dicts)


def sort_slices(slices):
    merged_dicts = merge_dicts(slices)
    return [merged_dicts[str(idx)] for idx in range(1, len(merged_dicts) + 1)]


# Return zero value corresponding to each data type
def get_default_value_by_type(type_str: str):
    # Does not handle enums or bytes
    if type_str == "float":
        return 0.0
    elif type_str == "long":
        return 0
    elif type_str == "boolean":
        return False
    elif type_str == "integer":
        return 0
    elif type_str == "string":
        return ""
    else:
        return None  # if invalid type, keep value as null


def findall(iterable, field):
    event_type, trace_type = field.split(".")
    result = [
        Row(
            **{
                trace_type: x["s3_proto_value"][event_type][trace_type],
                "time": x["time"],
            }
        )
        for x in iterable
    ]  # extract all trace slices of a specific event trace type
    return result


def deduplicate(iterable, field, algorithm_version):
    event_type, trace_type = field.split(".")
    if iterable[trace_type] is None:  # handle case where slice is null
        return {"time": iterable["time"], trace_type: None}
    if (
        len(iterable[trace_type]) == 1 and iterable[trace_type][0] == None
    ):  # handle case where slice is a list of a single null
        return {"time": iterable["time"], trace_type: None}

    # don't need to dedupe if we're on he version > 1
    if algorithm_version is not None and algorithm_version > 1:
        return {
            "time": iterable["time"],
            trace_type: [row.asDict(True) for row in iterable[trace_type]],
        }

    result = []
    unique_keys = set()
    for row in iterable[trace_type]:
        if row is None:  # handle case where a data-point in a trace slice is null
            continue
        row_dict = row.asDict(True)  # convert Row object to dict
        t = row_dict.get("offset_from_event_ms") or row_dict.get(
            "event_offset_ms"
        )  # use event offset time as the key for deduping
        if t in unique_keys:
            continue
        else:
            unique_keys.add(t)
            result.append(row_dict)
    return {"time": iterable["time"], trace_type: result}


def fill_nulls(iterable, field, schema):
    event_type, trace_type = field.split(".")
    event_trace_struct_schema = (
        schema.dataType[event_type].dataType[trace_type].dataType.elementType
    )
    result = []
    if iterable[trace_type] is None:  # handle case where slice is null
        return {
            trace_type: [dict.fromkeys(event_trace_struct_schema.fieldNames())],
            "time": iterable["time"],
        }  # return a slice w/ single default data-point
    for element in iterable[trace_type]:
        element_copy = copy.copy(element)
        if element_copy is None:
            element_copy = dict.fromkeys(
                event_trace_struct_schema.fieldNames()
            )  # append a default data-point

        for k, v in element_copy.items():
            if v is None:
                struct_field_type = event_trace_struct_schema[k].dataType.typeName()
                element_copy[k] = get_default_value_by_type(
                    struct_field_type
                )  # fill nulls with default value by struct type
        result.append(element_copy)
    return {"time": iterable["time"], trace_type: result}


def merge_trace_data(iterable, field, schema):
    event_type, trace_type = field.split(".")
    event_trace_struct_schema = (
        schema.dataType[event_type].dataType[trace_type].dataType.elementType
    )
    event_offset_ms_key = (
        "event_offset_ms"
        if "event_offset_ms" in event_trace_struct_schema.fieldNames()
        else "offset_from_event_ms"
    )

    for trace_slice in iterable:
        trace_slice_data = sorted(
            trace_slice[trace_type], key=lambda x: x[event_offset_ms_key]
        )  # sort each slices structs by event offset time
        timestamp_ms_key = "timestamp_ms"
        for element in trace_slice_data:  #  fill timestamps
            if (
                element[event_offset_ms_key] is None
            ):  # handle cases when entire slice was null or slice was a list of a single null
                element[timestamp_ms_key] = None
            else:
                element[timestamp_ms_key] = (
                    element[event_offset_ms_key] + trace_slice["time"]
                )
        trace_slice[trace_type] = trace_slice_data
    merged_trace_data = [x[trace_type] for x in iterable[:1]]
    for trace_slice in iterable[1:]:
        # Append current slice if last merged slice is an empty slice
        if merged_trace_data[-1][-1][timestamp_ms_key] is None:
            merged_trace_data.append(trace_slice[trace_type])
            continue

        for idx, element in enumerate(trace_slice[trace_type]):
            if element[timestamp_ms_key] is None:
                continue
            if element[timestamp_ms_key] <= merged_trace_data[-1][-1][timestamp_ms_key]:
                continue
            break
        merged_trace_data.append(trace_slice[trace_type][idx:])
    return merged_trace_data


def gen_event_trace_data_timeseries(iterable):
    result = defaultdict(list)
    for element in iterable:
        for k, v in element.items():
            result[k].append(v)
    return result


def transform_event_trace(
    s3_proto_value_list, field, s3_proto_value_list_schema, algorithm_version
):
    assert len(field.split(".")) == 2
    event_trace_data = findall(s3_proto_value_list, field)
    event_trace_data = list(
        map(lambda x: deduplicate(x, field, algorithm_version), event_trace_data)
    )
    event_trace_data = list(
        map(
            lambda x: fill_nulls(x, field, s3_proto_value_list_schema), event_trace_data
        )
    )
    event_trace_data = merge_trace_data(
        event_trace_data, field, s3_proto_value_list_schema
    )
    event_trace_data = list(
        map(lambda x: gen_event_trace_data_timeseries(x), event_trace_data)
    )
    return event_trace_data


def gen_timeseries_struct(field, s3_proto_value_schema):
    event_type, trace_type = field.split(".")
    event_trace_struct_schema = (
        s3_proto_value_schema.dataType[event_type]
        .dataType[trace_type]
        .dataType.elementType
    )
    event_offset_ms_key = (
        "event_offset_ms"
        if "event_offset_ms" in event_trace_struct_schema.fieldNames()
        else "offset_from_event_ms"
    )
    timestamp_ms_key = "timestamp_ms"
    result_struct = T.StructType([])
    result_struct.add(
        timestamp_ms_key,
        data_type=T.ArrayType(event_trace_struct_schema[event_offset_ms_key].dataType),
        nullable=True,
    )
    for struct_field in event_trace_struct_schema:
        result_struct.add(
            struct_field.name,
            data_type=T.ArrayType(struct_field.dataType),
            nullable=True,
        )
    return result_struct


START_DATE = dbutils.widgets.get("start_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
END_DATE = dbutils.widgets.get("end_date") or str(
    (datetime.utcnow() - timedelta(days=1)).date()
)
NUM_BATCHES = int(dbutils.widgets.get("num_batches")) or 5


df_stg = spark.table("kinesisstats.osdaccelerometer_with_s3_big_stat").where(
    F.col("date").between(START_DATE, END_DATE)
)

df_stg = df_stg.withColumn("batch_id", F.hash("org_id") % NUM_BATCHES)
df_stg.write.partitionBy("batch_id").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("datascience.stg_osdaccelerometer_event_traces")

for batch_id in range(NUM_BATCHES):
    print(f"Running Batch: {batch_id+1} of {NUM_BATCHES}")
    start = datetime.now()
    print(f"Starting at {start}...")
    df = spark.table("datascience.stg_osdaccelerometer_event_traces")
    df = df.where(df.batch_id == batch_id)
    df = df.repartition(4096)
    org_ids = [row["org_id"] for row in df.select("org_id").distinct().collect()]

    df = df.select("*", F.col("value.int_value"), F.lit(1).alias("trace_slice_idx"))
    df = df.withColumn(
        "value",
        F.struct(
            F.col("s3_proto_value.accel_event_data.is_debug"),
            F.col("s3_proto_value.accel_event_data.sample_rate_hz"),
            F.col("s3_proto_value.accel_event_data.oriented_harsh_detector_triggered"),
            *[
                F.col(f"value.{field}")
                for field in df.schema["value"].dataType.fieldNames()
            ],
        ),
    )
    df = df.withColumn(
        "value_list",
        F.array(F.create_map(F.col("trace_slice_idx").cast("string"), F.col("value"))),
    )
    df = df.withColumn(
        "s3_proto_value_list",
        F.array(
            F.create_map(
                F.col("trace_slice_idx").cast("string"),
                F.struct(F.col("s3_proto_value"), F.col("time")),
            )
        ),
    )

    VALUE_LIST_SCHEMA = df.schema["value_list"]
    S3_PROTO_VALUE_LIST_SCHEMA = df.schema["s3_proto_value_list"]
    S3_PROTO_VALUE_SCHEMA = S3_PROTO_VALUE_LIST_SCHEMA.dataType.elementType.valueType[
        "s3_proto_value"
    ]

    df = df.withColumn(
        "value_list",
        F.udf(
            lambda x: sort_slices(x),
            returnType=T.ArrayType(VALUE_LIST_SCHEMA.dataType.elementType.valueType),
        )(F.col("value_list")),
    )
    df = df.withColumn(
        "s3_proto_value_list",
        F.udf(
            lambda x: sort_slices(x),
            returnType=T.ArrayType(
                S3_PROTO_VALUE_LIST_SCHEMA.dataType.elementType.valueType
            ),
        )(F.col("s3_proto_value_list")),
    )

    df = df.withColumn(
        "recent_accel_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.recent_accel", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.recent_accel", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )

    df = df.withColumn(
        "recent_gps_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.recent_gps", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.recent_gps", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )

    df = df.withColumn(
        "accel_pedal_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.accel_pedal", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.accel_pedal", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "brake_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.brake", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct("accel_event_data.brake", S3_PROTO_VALUE_SCHEMA)
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "recent_gyro_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.recent_gyro", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.recent_gyro", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "ecu_speed_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.ecu_speed", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.ecu_speed", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "cruise_control_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x, "accel_event_data.cruise_control", S3_PROTO_VALUE_SCHEMA, he_version
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.cruise_control", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "forward_vehicle_speed_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.forward_vehicle_speed",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.forward_vehicle_speed", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "forward_vehicle_distance_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.forward_vehicle_distance",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.forward_vehicle_distance", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "oriented_accel_raw_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.oriented_accel_raw",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.oriented_accel_raw", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "oriented_gyro_raw_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.oriented_gyro_raw",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.oriented_gyro_raw", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "oriented_accel_lp_filtered_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.oriented_accel_lp_filtered",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.oriented_accel_lp_filtered", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )
    df = df.withColumn(
        "oriented_gyro_lp_filtered_trace",
        F.udf(
            lambda x, he_version: transform_event_trace(
                x,
                "accel_event_data.oriented_gyro_lp_filtered",
                S3_PROTO_VALUE_SCHEMA,
                he_version,
            ),
            returnType=T.ArrayType(
                gen_timeseries_struct(
                    "accel_event_data.oriented_gyro_lp_filtered", S3_PROTO_VALUE_SCHEMA
                )
            ),
        )(
            F.col("s3_proto_value_list"),
            F.col(
                "value.proto_value.accelerometer_event.imu_harsh_event.algorithm_version"
            ),
        ),
    )

    df = df.select(
        "date",
        "stat_type",
        "org_id",
        "object_type",
        "object_id",
        "time",
        "value_list",
        "recent_accel_trace",
        "recent_gps_trace",
        "brake_trace",
        "recent_gyro_trace",
        "ecu_speed_trace",
        "cruise_control_trace",
        "forward_vehicle_speed_trace",
        "forward_vehicle_distance_trace",
        "oriented_accel_raw_trace",
        "oriented_gyro_raw_trace",
        "oriented_accel_lp_filtered_trace",
        "oriented_gyro_lp_filtered_trace",
    )

    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").option(
        "replaceWhere",
        f"date >= '{START_DATE}' AND date <= '{END_DATE}' and org_id in {tuple(org_ids)}",
    ).saveAsTable(
        "dataprep.osdaccelerometer_event_traces", partitionBy=["date", "org_id"]
    )
    print(f"...completed at {datetime.now()}...took {datetime.now() - start}")
