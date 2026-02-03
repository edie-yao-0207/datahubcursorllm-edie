# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_stills_inference_processor"

# COMMAND ----------

inference_processor = TripStillsInferenceProcessor(
    "us_continental_telemetry_second_quarter_second"
)

# COMMAND ----------

inference_processor.process_inference()

# COMMAND ----------
