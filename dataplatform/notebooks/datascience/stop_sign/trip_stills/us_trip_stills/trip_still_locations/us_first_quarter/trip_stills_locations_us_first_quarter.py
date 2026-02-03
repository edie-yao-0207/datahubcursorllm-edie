# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_loc_gen"

# COMMAND ----------

CITY_NAME = "us_continental_telemetry_first_quarter"

# COMMAND ----------

city_gen = TripStillLocGen(CITY_NAME)

# COMMAND ----------

city_gen.run_and_save()

# COMMAND ----------
