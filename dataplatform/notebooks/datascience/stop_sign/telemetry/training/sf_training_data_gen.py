# Databricks notebook source
# MAGIC %run backend/datascience/stop_sign/telemetry/region_telemetry_generator

# COMMAND ----------

# bounds determined from google maps
CITY_NAME = "sf"
NW_LAT = 37.811461
NW_LON = -122.535540
SE_LAT = 37.687934
SE_LON = -122.347934

# COMMAND ----------

region_telemetry_generator = RegionTelemetryGenerator(
    NW_LAT, NW_LON, SE_LAT, SE_LON, CITY_NAME
)

# COMMAND ----------

region_telemetry_generator.label_telemetry_data()

# COMMAND ----------

region_telemetry_generator.generate_histograms(training=True, clear_histograms=True)

# COMMAND ----------
