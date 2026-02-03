# Databricks notebook source
# MAGIC %run "/backend/datascience/stop_sign/utils"

# COMMAND ----------

from pyspark.sql.functions import from_json, upper
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------


def process_here_df(here_df):
    here_df["geometry"] = here_df["geometry"].apply(str)
    here_df = here_df.rename(
        columns={"@ns:com:here:xyz": "xyz", "@ns:com:here:mom:meta": "meta"}
    )
    here_df["xyz"] = here_df["xyz"].apply(str)
    return here_df


def pt_str_to_pt(pt_str):
    pt_str = pt_str.lstrip()
    pt_split = pt_str.split(" ")
    p0 = float(pt_split[0])
    p1 = float(pt_split[1])
    p2 = float(pt_split[2])
    return [p0, p1, p2]


@udf("array<double>")
def get_ref_node(lstr):
    lstr_formatted = lstr.replace("LINESTRING Z ", "")
    lstr_formatted = lstr_formatted.replace("(", "")
    lstr_formatted = lstr_formatted.replace(")", "")
    lstr_split = lstr_formatted.split(",")
    min_node = pt_str_to_pt(lstr_split[0])
    for pt_str in lstr_split:
        pt = pt_str_to_pt(pt_str)
        if pt[1] < min_node[1]:
            min_node = pt
        elif pt[1] == min_node[1] and pt[0] < min_node[0]:
            min_node = pt
        elif pt[1] == min_node[1] and pt[0] == min_node[0] and pt[2] < min_node[2]:
            min_node = pt
    return min_node


@udf("array<double>")
def get_non_ref_node(lstr):
    lstr_formatted = lstr.replace("LINESTRING Z ", "")
    lstr_formatted = lstr_formatted.replace("(", "")
    lstr_formatted = lstr_formatted.replace(")", "")
    lstr_split = lstr_formatted.split(",")
    min_node = pt_str_to_pt(lstr_split[0])
    for pt_str in lstr_split:
        pt = pt_str_to_pt(pt_str)
        if pt[1] > min_node[1]:
            min_node = pt
        elif pt[1] == min_node[1] and pt[0] > min_node[0]:
            min_node = pt
        elif pt[1] == min_node[1] and pt[0] == min_node[0] and pt[2] > min_node[2]:
            min_node = pt
    return min_node


def process_node_col(data_sdf, col_name):
    data_sdf = data_sdf.withColumn(f"{col_name}_lon", col(col_name).getItem(0))
    data_sdf = data_sdf.withColumn(f"{col_name}_lat", col(col_name).getItem(1))
    data_sdf = data_sdf.withColumn(f"{col_name}_z", col(col_name).getItem(2))
    return data_sdf


# COMMAND ----------

navigable_roads_geojson = "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/ground_truth_datasets/houston/houston_navigableroads_v67_3_JYPsV27J.geojson"
houston_navigable_roads = process_here_df(geopandas.read_file(navigable_roads_geojson))

# COMMAND ----------

houston_navigable_roads_sdf_raw = spark.createDataFrame(
    houston_navigable_roads[["id", "geometry", "startNode", "endNode", "roads"]]
)

# COMMAND ----------

roads_schema = ArrayType(
    StructType(
        [
            StructField(
                "roadName",
                StructType(
                    [
                        StructField("name", StringType()),
                        StructField("baseName", StringType()),
                        StructField("nameType", StringType()),
                        StructField("streetType", StringType()),
                        StructField("languageCode", StringType()),
                    ]
                ),
            ),
            StructField(
                "addressRanges",
                ArrayType(
                    StructType(
                        [
                            StructField("type", StringType()),
                            StructField("leftRange", ArrayType(StringType())),
                            StructField("leftFormat", StringType()),
                            StructField("leftScheme", StringType()),
                            StructField("rightRange", ArrayType(StringType())),
                            StructField("rightFormat", StringType()),
                            StructField("rightScheme", StringType()),
                        ]
                    )
                ),
            ),
        ]
    )
)

houston_navigable_roads_sdf_roads = houston_navigable_roads_sdf_raw.withColumn(
    "roads", from_json(col("roads"), roads_schema)
)
houston_navigable_roads_sdf_roads = houston_navigable_roads_sdf_roads.withColumn(
    "fixed_street", upper(col("roads").getItem(0).roadName.baseName)
)

# COMMAND ----------

houston_navigable_roads_sdf = houston_navigable_roads_sdf_roads.withColumn(
    "ref_node", get_ref_node(col("geometry"))
)
houston_navigable_roads_sdf = houston_navigable_roads_sdf.withColumn(
    "non_ref_node", get_non_ref_node(col("geometry"))
)

# COMMAND ----------

houston_navigable_roads_sdf = process_node_col(houston_navigable_roads_sdf, "ref_node")
houston_navigable_roads_sdf = process_node_col(
    houston_navigable_roads_sdf, "non_ref_node"
)

# COMMAND ----------

houston_navigable_roads_bearing = houston_navigable_roads_sdf.withColumn(
    "bearing",
    calculate_bearing(
        col("ref_node_lon"),
        col("ref_node_lat"),
        col("non_ref_node_lon"),
        col("non_ref_node_lat"),
    ),
)
houston_navigable_roads_bearing = houston_navigable_roads_bearing.withColumn(
    "reverse_bearing", calculate_reverse_bearing(col("bearing"))
)

# COMMAND ----------

houston_navigable_roads_bearing.write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("stopsigns.houston_navigable_roads_here_layer")
