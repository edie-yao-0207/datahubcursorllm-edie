# Databricks notebook source
# MAGIC %run backend/datascience/stop_sign/telemetry/region_telemetry_generator

# COMMAND ----------

# bounds determined from google maps
CITY_NAME = "seattle"
NW_LAT = 47.735188
NW_LON = -122.438582
SE_LAT = 47.487539
SE_LON = -122.028255

# COMMAND ----------

region_telemetry_generator = RegionTelemetryGenerator(
    NW_LAT, NW_LON, SE_LAT, SE_LON, CITY_NAME
)

# COMMAND ----------

region_telemetry_generator.label_telemetry_data()

# COMMAND ----------

region_telemetry_generator.generate_histograms(training=True, clear_histograms=False)

# COMMAND ----------
