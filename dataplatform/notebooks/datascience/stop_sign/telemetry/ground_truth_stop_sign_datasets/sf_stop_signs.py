# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/telemetry/stop_sign_gen"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Stop Sign Data

# COMMAND ----------


@udf("double")
def calculate_bearing(st_facing):
    st_to_bearing = {"NB": 0.0, "EB": 90.0, "SB": 180.0, "WB": -90.0}
    return st_to_bearing[st_facing]


@udf
def point_to_lat(point):
    return point.split(" ")[2][:-1]


@udf
def point_to_lon(point):
    return point.split(" ")[1][1:]


# COMMAND ----------

f = "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/ground_truth_datasets/sf/Stop_Signs.csv"
stop_signs_df = geopandas.read_file(f)
stop_signs_df["point"] = stop_signs_df["point"].apply(str)
stop_signs_df["geometry"] = stop_signs_df["geometry"].apply(str)
stop_signs_sdf = spark.createDataFrame(stop_signs_df)

stop_signs_sdf = stop_signs_sdf.select(
    "STREET", "X_STREET", "DIRECTION", "ST_FACING", "point"
)
stop_signs_sdf = stop_signs_sdf.withColumn(
    "latitude", point_to_lat(col("point")).cast("double")
)
stop_signs_sdf = stop_signs_sdf.withColumn(
    "longitude", point_to_lon(col("point")).cast("double")
)
stop_signs_sdf = stop_signs_sdf.withColumn("fixed_street", fix_street(col("STREET")))
stop_signs_sdf = stop_signs_sdf.withColumn(
    "fixed_x_street", fix_street(col("X_STREET"))
)
stop_signs_sdf = stop_signs_sdf.withColumn(
    "bearing", calculate_bearing(col("ST_FACING"))
)
stop_signs_sdf = stop_signs_sdf.select(
    "latitude", "longitude", "fixed_street", "fixed_x_street", "bearing"
)

# COMMAND ----------

# Stop signs missing in the dataset found through manual investigation
manual_stop_signs = [
    [37.747002300000005, -122.4060886, "YORK", "MONTCALM", 135.0],
    [37.747002300000005, -122.4060886, "YORK", "MONTCALM", -45.0],
    [37.7649698, -122.39208210000001, "SOUTHERN CONNECTOR", "OWENS", -90.0],
    [37.7472383, -122.42139250000001, "27TH", "SAN JOSE", 90.0],
    [37.769258900000004, -122.46810900000001, "CONCOURSE", "MUSIC CONCOURSE", 45.0],
    [
        37.769258900000004,
        -122.46810900000001,
        "MUSIC CONCOURSE",
        "MUSIC CONCOURSE",
        -135.0,
    ],
    [
        37.769258900000004,
        -122.46810900000001,
        "MUSIC CONCOURSE",
        "MUSIC CONCOURSE",
        135.0,
    ],
    [37.7295753, -122.45962370000001, "NORTHWOOD", "PIZARRO", -135.0],
    [37.7295753, -122.45962370000001, "NORTHWOOD", "PIZARRO", 45.0],
    [37.7831506, -122.43100620000001, "", "WEBSTER", 15.0],
    [37.7464424, -122.4027686, "JERROLD", "BARNEVELD", 135.0],
    [37.7464424, -122.4027686, "JERROLD", "BARNEVELD", -45.0],
    [37.7464424, -122.4027686, "BARNEVELD", "JERROLD", 20.0],
    [37.7703799, -122.46030510000001, "NANCY PELOSI", "BOWLING GREEN", 80.0],
    [37.7703799, -122.46030510000001, "NANCY PELOSI", "BOWLING GREEN", -90.0],
    [37.7703799, -122.46030510000001, "BOWLING GREEN", "NANCY PELOSI", 0.0],
]

cols = ["latitude", "longitude", "fixed_street", "fixed_x_street", "bearing"]
manual_stop_signs_sdf = spark.createDataFrame(data=manual_stop_signs, schema=cols)
stop_signs_sdf = stop_signs_sdf.unionByName(manual_stop_signs_sdf)

# COMMAND ----------


def ints_filter_fn(ints_sdf):
    filtered_ints = ints_sdf.filter(
        col("latitude") >= 37.70825102382599
    )  # filter out intersections below vistacion valley, not in ground truth
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(37.790898, 37.806762))
        | ~(col("longitude").between(-122.483679, -122.4512337))
    )  # filter out presidio
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(37.766762, 37.773344))
        | ~(col("longitude").between(-122.395950, -122.387553))
    )  # filter out mission bay ucsf, we don't have ground truth there
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(37.703910, 37.722402))
        | ~(col("longitude").between(-122.500885, -122.486978))
    )  # filter out fort funston area
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(37.803532, 37.811873))
        | ~(col("longitude").between(-122.478651, -122.466978))
    )  # filter out GGB entrance
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(37.805684, 37.818081))
        | ~(col("longitude").between(-122.374806, -122.356310))
    )  # filter out Yerba Buena island

    return filtered_ints


# COMMAND ----------

stop_sign_generator = CityStopSignGenerator("sf", stop_signs_sdf, ints_filter_fn)

# COMMAND ----------

stop_sign_generator.run_all()

# COMMAND ----------
