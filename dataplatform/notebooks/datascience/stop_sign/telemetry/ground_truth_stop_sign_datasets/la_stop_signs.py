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

# 0-360 to -180 - 180
# https://gis.stackexchange.com/questions/201789/verifying-formula-that-will-convert-longitude-0-360-to-180-to-180
@udf("integer")
def flip_and_convert_360_to_180(deg):
    to_convert = deg - 180
    if deg < 180:
        to_convert = deg + 180
    return ((to_convert + 180) % 360) - 180


# COMMAND ----------

f = "/dbfs/mnt/samsara-databricks-playground/stop_sign_detection/ground_truth_datasets/la/Stop_and_Yield_Signs.csv"
stop_signs_sdf = spark.read.csv(f, header=True, inferSchema=True)
# Include only stop signs (sign type = 1)
stop_signs_sdf = stop_signs_sdf.filter(col("SIGN_TYPE") == 1)
stop_signs_sdf = stop_signs_sdf.withColumnRenamed("X", "longitude")
stop_signs_sdf = stop_signs_sdf.withColumnRenamed("Y", "latitude")
# shift bearing to be -180 <-> 180 since that's the range for geod. The LA data is 0 to 360
# we also have to flip the bearing because FACE_DIREC is actually in the opposite direction of the traffic that it stops
stop_signs_sdf = stop_signs_sdf.withColumn(
    "bearing", flip_and_convert_360_to_180(col("FACE_DIREC")).cast("double")
)
stop_signs_sdf = stop_signs_sdf.withColumn(
    "fixed_street", lit("x")
)  # dummy street name for now
stop_signs_sdf = stop_signs_sdf.select(
    "longitude", "latitude", "bearing", "fixed_street"
)

# COMMAND ----------

# Bounds generated manually by examining Kepler visualization of source data
def ints_filter_fn(ints_sdf):
    filtered_ints = ints_sdf.filter(
        (col("latitude").between(33.7067466712264, 34.3289083785999))
        & (col("longitude").between(-118.664645396241, -118.160388166117))
    )  # keep la boundaries
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.1546134574283, 34.2064703482728))
        | ~(col("longitude").between(-118.350035312813, -118.184981885783))
    )  # filter out burbank
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.1531403248588, 34.2503994202844))
        | ~(col("longitude").between(-118.265911436215, -118.15664074729))
    )  # filter out la canada
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.750183213124, 33.9310870292109))
        | ~(col("longitude").between(-118.444536874601, -118.30906904849))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.865476636837, 33.9373749914778))
        | ~(col("longitude").between(-118.306952420654, -118.291342368831))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.8047246791834, 33.9235983056732))
        | ~(col("longitude").between(-118.281796745, 118.23797680474))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.2346485, 34.288728))
        | ~(col("longitude").between(-118.6646195, -118.63218950000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.289566400000005, 34.3287396))
        | ~(col("longitude").between(-118.4050213, -118.1612506))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.2064812, 34.229770800000004))
        | ~(col("longitude").between(-118.3363353, -118.26620720000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.1620531, 34.195723))
        | ~(col("longitude").between(-118.35240820000001, -118.35018160000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.145115000000004, 34.1544125))
        | ~(col("longitude").between(-118.34619160000001, -118.32213680000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.1250577, 34.152921500000005))
        | ~(col("longitude").between(-118.26300900000001, -118.22715140000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.1257114, 34.1523199))
        | ~(col("longitude").between(-118.1834673, -118.1603893))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0991619, 34.122421))
        | ~(col("longitude").between(-118.1694425, -118.16041560000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0396598, 34.1440122))
        | ~(col("longitude").between(-118.66388280000001, -118.57651340000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.144457100000004, 34.1652238))
        | ~(col("longitude").between(-118.6631007, -118.6450611))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.057734100000005, 34.071677300000005))
        | ~(col("longitude").between(-118.4173083, -118.3847655))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0692284, 34.090607600000006))
        | ~(col("longitude").between(-118.4238765, -118.39041440000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0758116, 34.0906346))
        | ~(col("longitude").between(-118.39352950000001, -118.37629600000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.996702400000004, 34.0261189))
        | ~(col("longitude").between(-118.51555110000001, -118.461143))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.024531700000004, 34.0386704))
        | ~(col("longitude").between(-118.50464480000001, -118.46609950000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9806906, 34.015089200000006))
        | ~(col("longitude").between(-118.3976564, -118.36236860000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9885937, 34.0048764))
        | ~(col("longitude").between(-118.35805010000001, -118.33474760000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9238249, 34.063447700000005))
        | ~(col("longitude").between(-118.1918706, -118.16165790000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9236219, 34.0132059))
        | ~(col("longitude").between(-118.22554430000001, -118.16041440000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9532599, 33.989321700000005))
        | ~(col("longitude").between(-118.25496450000001, -118.16050190000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.923733, 33.9289964))
        | ~(col("longitude").between(-118.25263720000001, -118.1612436))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9312771, 33.9544015))
        | ~(col("longitude").between(-118.44402470000001, -118.39531500000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.931343500000004, 33.9472532))
        | ~(col("longitude").between(-118.3972179, -118.3685171))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9311738, 33.980777100000005))
        | ~(col("longitude").between(-118.37033260000001, -118.3352098))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9311738, 33.9663957))
        | ~(col("longitude").between(-118.37033260000001, -118.31880740000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.9311738, 33.938210000000005))
        | ~(col("longitude").between(-118.37033260000001, -118.30795780000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.863355600000006, 33.9382171))
        | ~(col("longitude").between(-118.30891940000001, -118.303638))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.7978103, 33.861412800000004))
        | ~(col("longitude").between(-118.3011642, -118.28187860000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.7654444, 33.7787646))
        | ~(col("longitude").between(-118.3090088, -118.28057940000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.7211025, 33.7501515))
        | ~(col("longitude").between(-118.4107417, -118.3180138))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.7157962, 33.7722008))
        | ~(col("longitude").between(-118.2752497, -118.1606067))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.7629189, 33.8044841))
        | ~(col("longitude").between(-118.2370728, -118.160588))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.765833, 33.804681))
        | ~(col("longitude").between(-118.2266563, -118.1604879))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0369157, 34.149030100000004))
        | ~(col("longitude").between(-118.66462170000001, -118.60517600000001))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(34.0138018, 34.0665015))
        | ~(col("longitude").between(-118.1922028, -118.16041850))
    )  # filter out suburbs
    filtered_ints = filtered_ints.filter(
        ~(col("latitude").between(33.989536300000005, 34.0155978))
        | ~(col("longitude").between(-118.2391688, -118.21996030000001))
    )  # filter out suburbs
    return filtered_ints


# COMMAND ----------

# Stop signs missing in the dataset found through manual investigation
manual_stop_signs = [
    [33.738795700000004, -118.29894060000001, 0, "x"],
    [33.738795700000004, -118.29894060000001, 90, "x"],
    [33.738795700000004, -118.29894060000001, -90, "x"],
    [33.738795700000004, -118.29894060000001, 180, "x"],
    [33.7555794, -118.28972420000001, -135, "x"],
    [33.7555794, -118.28972420000001, 45, "x"],
    [33.7555794, -118.28972420000001, -20, "x"],
    [33.736093600000004, -118.30776970000001, 90, "x"],
    [33.736093600000004, -118.30776970000001, -90, "x"],
    [33.7388057, -118.30651130000001, -90, "x"],
    [33.7388057, -118.30651130000001, 90, "x"],
    [33.7388057, -118.30651130000001, 0, "x"],
    [33.729773800000004, -118.3097398, 0, "x"],
    [33.729773800000004, -118.3097398, 180, "x"],
    [33.913335100000005, -118.2893621, 180, "x"],
    [33.913335100000005, -118.2893621, 0, "x"],
    [33.9334032, -118.2279264, -63, "x"],
    [33.9334032, -118.2279264, -153, "x"],
    [33.774638700000004, -118.23925720000001, 90, "x"],
    [33.774638700000004, -118.23925720000001, -90, "x"],
    [33.774638700000004, -118.23925720000001, 0, "x"],
    [33.774638700000004, -118.23925720000001, 180, "x"],
    [33.7105842, -118.2928464, 180, "x"],
    [33.7105842, -118.2928464, 0, "x"],
    [34.081796000000004, -118.35841230000001, 0, "x"],
    [34.081796000000004, -118.35841230000001, 180, "x"],
]

cols = ["latitude", "longitude", "bearing", "fixed_street"]
manual_stop_signs_sdf = spark.createDataFrame(data=manual_stop_signs, schema=cols)
stop_signs_sdf = stop_signs_sdf.unionByName(manual_stop_signs_sdf)

# COMMAND ----------

stop_sign_generator = CityStopSignGenerator("la", stop_signs_sdf, ints_filter_fn)

# COMMAND ----------

stop_sign_generator.run_all()

# COMMAND ----------
