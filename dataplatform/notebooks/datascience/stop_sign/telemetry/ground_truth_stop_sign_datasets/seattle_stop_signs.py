# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/telemetry/stop_sign_gen"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load In Data

# COMMAND ----------

# Stop signs missing in the dataset found through manual investigation
manual_stop_signs = {
    (47.5654026176198, -122.36552466638, 73972): 90,
    (47.5881107544454, -122.33645852285, 74567): 180,
    (47.6016658882623, -122.332764461079, 81601): 0,
    (47.6017971743962, -122.332731700173, 81725): 90,
    (47.6017971743962, -122.332731700173, 82050): -90,
    (47.5428323676551, -122.370607458168, 83693): 135,
    (47.7192694886393, -122.288599124532, 84217): 180,
    (47.6607390223515, -122.392848798861, 85660): 0,
    (47.7229288571658, -122.288659943415, 129901): 90,
    (47.5798498413383, -122.352283025513, 174106): 0,
}


@udf("integer")
def bearing_from_facing(facing, lat, lon, obj_id):
    # Some stop signs don't have a facing in the CSV, I just manually determined the bearing for these through Google Maps
    if facing == " ":
        return manual_stop_signs[(lat, lon, obj_id)]

    # We actually want the opposite bearing because traffic travels the opposite direction the stop sign is facing
    facing_to_bearing = {
        "N": 180,
        "NE": -135,
        "E": -90,
        "SE": -45,
        "S": 0,
        "SW": 45,
        "W": 90,
        "NW": 135,
    }
    return facing_to_bearing[facing]


@udf
def get_street_name(unitname):
    unitname_split = unitname.split(" ")
    direction_names = {
        "N": "NORTH",
        "NE": "NORTHEAST",
        "E": "EAST",
        "SE": "SOUTHEAST",
        "S": "SOUTH",
        "SW": "SOUTHWEST",
        "W": "WEST",
        "NW": "NORTHWEST",
    }
    if unitname_split[0] not in direction_names:
        return unitname_split[0]
    return direction_names[unitname[0]] + " " + unitname_split[1]


# COMMAND ----------

f = "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/ground_truth_datasets/seattle/Street_Signs.csv"
stop_signs_sdf = (
    spark.read.csv(f, header=True, inferSchema=True)
    .filter(col("TEXT") == "STOP")
    .select("X", "Y", "OBJECTID", "FACING", "UNITDESC")
)
stop_signs_sdf = stop_signs_sdf.withColumnRenamed("X", "longitude")
stop_signs_sdf = stop_signs_sdf.withColumn("longitude", col("longitude").cast("double"))
stop_signs_sdf = stop_signs_sdf.withColumnRenamed("Y", "latitude")
stop_signs_sdf = stop_signs_sdf.withColumn("latitude", col("latitude").cast("double"))
stop_signs_sdf = stop_signs_sdf.withColumn("OBJECTID", col("OBJECTID").cast("int"))
stop_signs_sdf = stop_signs_sdf.filter(
    (col("latitude") != 47.6036246083544) & (col("longitude") != -122.338441477289)
)  # particularly strange and messy intersection
stop_signs_sdf = stop_signs_sdf.withColumn(
    "bearing",
    bearing_from_facing(
        col("FACING"), col("latitude"), col("longitude"), col("OBJECTID")
    ).cast("double"),
).drop("FACING", "OBJECTID")
stop_signs_sdf = stop_signs_sdf.withColumn(
    "fixed_street", get_street_name(col("UNITDESC"))
).drop("UNITDESC")

# COMMAND ----------


def ints_filter_fn(ints_sdf):
    filtered_ints = ints_sdf.filter(
        (col("latitude").between(47.4956598552436, 47.7340992797510))
        & (col("longitude").between(-122.430647793762, -122.237194458492))
    )  # keep seattle boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.4988958617145, 47.5155651305104))
        | ~(col("longitude").between(-122.370489031599, -122.290935481775))
    )  # filter out some golf coruses, we don't have ground truth there
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.495739, 47.499643000000006))
        | ~(col("longitude").between(-122.37230380000001, -122.278345))
    )  # out of boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.4956654, 47.5070795))
        | ~(col("longitude").between(-122.29948150000001, -122.2725278))
    )  # out of boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.49582100000001, 47.519220000000004))
        | ~(col("longitude").between(-122.31798350000001, -122.29417380000001))
    )  # out of boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.5312661, 47.6446929))
        | ~(col("longitude").between(-122.25465190000001, -122.2372818))
    )  # out of boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.701183900000004, 47.7340413))
        | ~(col("longitude").between(-122.260765, -122.23731860000001))
    )  # out of boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(47.495666400000005, 47.4988067))
        | ~(col("longitude").between(-122.2586045, -122.2382628))
    )  # out of boundaries
    return filtered_ints


# COMMAND ----------

stop_sign_generator = CityStopSignGenerator("seattle", stop_signs_sdf, ints_filter_fn)

# COMMAND ----------

stop_sign_generator.run_all()


# COMMAND ----------

display(spark.table("playground.seattle_non_stop_signs_osm"))

# COMMAND ----------
