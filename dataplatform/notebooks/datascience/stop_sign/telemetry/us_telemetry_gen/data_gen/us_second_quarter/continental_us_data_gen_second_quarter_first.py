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
CITY_NAME = "us_continental_telemetry_second_quarter_first"
NW_LAT = 30.893936325  # 24.7433195 + (49.3457868 - 24.7433195)/4
NW_LON = -110.326151225  # -124.7844079 + (124.7844079 - 66.9513812)/4
SE_LAT = 24.7433195
SE_LON = -95.86789455  # -124.7844079 + (124.7844079 - 66.9513812)/2

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
