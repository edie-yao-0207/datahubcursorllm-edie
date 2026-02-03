# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_loc_gen"

# COMMAND ----------

CITY_NAME = "us_continental_telemetry_fourth_quarter_second"

# COMMAND ----------

city_gen = TripStillLocGen(CITY_NAME)

# COMMAND ----------

city_gen.run_and_save()

# COMMAND ----------
