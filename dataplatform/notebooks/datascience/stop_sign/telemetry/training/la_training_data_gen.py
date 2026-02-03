# Databricks notebook source
# MAGIC %run backend/datascience/stop_sign/telemetry/region_telemetry_generator

# COMMAND ----------

# bounds determined from google maps
CITY_NAME = "la"
NW_LAT = 34.334590
NW_LON = -118.684902
SE_LAT = 33.710533
SE_LON = -118.040441

# COMMAND ----------

region_telemetry_generator = RegionTelemetryGenerator(
    NW_LAT, NW_LON, SE_LAT, SE_LON, CITY_NAME
)

# COMMAND ----------

region_telemetry_generator.label_telemetry_data()

# COMMAND ----------

region_telemetry_generator.generate_histograms(training=True, clear_histograms=False)

# COMMAND ----------
