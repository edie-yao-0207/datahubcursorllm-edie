# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_inference"

# COMMAND ----------

trip_still_inference = TripStillInference(
    "us_continental_telemetry_fourth_quarter_first"
)

# COMMAND ----------

# indices that have failed:
trip_still_inference.run_inference(indices_to_exclude=[])

# COMMAND ----------
