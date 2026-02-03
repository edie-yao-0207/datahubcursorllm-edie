# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run backend/datascience/stop_sign/telemetry/region_telemetry_generator

# COMMAND ----------

# # http://en.wikipedia.org/wiki/Extreme_points_of_the_United_States#Westernmost
# top = 49.3457868 # north lat
# left = -124.7844079 # west long
# right = -66.9513812 # east long
# bottom =  24.7433195 # south lat
CITY_NAME = "us_continental_telemetry_fourth_quarter_fourth"
NW_LAT = 49.3457868
NW_LON = -81.409637875  # -124.7844079 + (124.7844079 - 66.9513812)*(3/4)
SE_LAT = 43.195169975  # 24.7433195 + (49.3457868 - 24.7433195)*(3/4)
SE_LON = -66.9513812

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate City Data

# COMMAND ----------

region_telemetry_generator = RegionTelemetryGenerator(
    NW_LAT, NW_LON, SE_LAT, SE_LON, CITY_NAME
)

# COMMAND ----------

# generate intersections
region_telemetry_generator.generate_intersections()

# COMMAND ----------

# generate location data
region_telemetry_generator.generate_location_data()

# COMMAND ----------

region_telemetry_generator.generate_telemetry_data()

# COMMAND ----------

region_telemetry_generator.generate_histograms(clear_histograms=False)

# COMMAND ----------

region_telemetry_generator.create_inference_table()

# COMMAND ----------
