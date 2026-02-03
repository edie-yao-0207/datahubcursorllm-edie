# Databricks notebook source
# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/telemetry/stop_sign_gen"

# COMMAND ----------

from pyspark.sql.functions import from_json, when
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# COMMAND ----------


def process_here_df(here_df):
    here_df["geometry"] = here_df["geometry"].apply(str)
    here_df = here_df.rename(
        columns={"@ns:com:here:xyz": "xyz", "@ns:com:here:mom:meta": "meta"}
    )
    here_df["xyz"] = here_df["xyz"].apply(str)
    return here_df


def process_pt_str(pt_str):
    return pt_str.replace("POINT Z (", "").split(" ")


@udf("double")
def get_lon_from_pt_str(pt_str):
    return float(process_pt_str(pt_str)[0])


@udf("double")
def get_lat_from_pt_str(pt_str):
    return float(process_pt_str(pt_str)[1])


REF_STR = "REF_END_OF_LINK"

# COMMAND ----------

geojson_file = "houston_trafficsigns_v67_UldhqNDV.geojson"
path = f"/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/ground_truth_datasets/houston/{geojson_file}"

houston_data = process_here_df(geopandas.read_file(path))

# COMMAND ----------

houston_data_raw_sdf = spark.createDataFrame(
    houston_data[
        [
            "id",
            "category",
            "signType",
            "endOfLink",
            "references",
            "featureType",
            "geometry",
        ]
    ]
)

# COMMAND ----------

houston_data_sdf = houston_data_raw_sdf.withColumn(
    "latitude", get_lat_from_pt_str(col("geometry"))
)
houston_data_sdf = houston_data_sdf.withColumn(
    "longitude", get_lon_from_pt_str(col("geometry"))
)
houston_data_sdf = houston_data_sdf.drop("geometry")
houston_data_sdf = houston_data_sdf.filter(col("signType") == "Stop")

# COMMAND ----------

schema = ArrayType(
    StructType(
        [
            StructField("ids", ArrayType(StringType()), False),
            StructField("layerId", StringType(), False),
        ]
    )
)
houston_data_ref_id = houston_data_sdf.withColumn(
    "references", from_json(col("references"), schema)
)
houston_data_ref_id = houston_data_ref_id.withColumn(
    "ref_id", col("references").getItem(0).ids.getItem(0)
)
houston_data_ref_id = houston_data_ref_id.drop("references")

# COMMAND ----------

houston_navigable_roads = spark.table("stopsigns.houston_navigable_roads_here_layer")

# COMMAND ----------

houston_stop_signs_bearing = houston_data_ref_id.join(
    houston_navigable_roads, houston_data_ref_id.ref_id == houston_navigable_roads.id
).select(
    houston_data_ref_id.id,
    "category",
    "signType",
    "endOfLink",
    "featureType",
    "latitude",
    "longitude",
    "ref_id",
    "bearing",
    "reverse_bearing",
    "fixed_street",
)
houston_stop_signs_bearing = houston_stop_signs_bearing.withColumn(
    "stop_bearing",
    when(col("endOfLink") == REF_STR, col("bearing")).otherwise(col("reverse_bearing")),
)
houston_stop_signs_bearing = houston_stop_signs_bearing.drop(
    "bearing", "reverse_bearing"
)
houston_stop_signs_bearing = houston_stop_signs_bearing.withColumnRenamed(
    "stop_bearing", "bearing"
)

# COMMAND ----------


def ints_filter_fn(ints_sdf):
    filtered_ints = ints_sdf.filter(
        (col("latitude").between(29.70705, 29.79660))
        & (col("longitude").between(-95.45060, -95.36119))
    )  # keep houston boundaries
    return filtered_ints


# COMMAND ----------

stop_sign_generator = CityStopSignGenerator(
    "houston", houston_stop_signs_bearing, ints_filter_fn
)

# COMMAND ----------

stop_sign_generator.run_all()
