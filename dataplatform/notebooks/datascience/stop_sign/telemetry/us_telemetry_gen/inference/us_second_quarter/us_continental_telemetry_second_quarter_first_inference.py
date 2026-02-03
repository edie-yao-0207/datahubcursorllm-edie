# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/telemetry/region_telemetry_inference"

# COMMAND ----------

region_telemetry_inference = RegionTelemetryInference(
    "us_continental_telemetry_second_quarter_first"
)

# COMMAND ----------

region_telemetry_inference.run_and_save()

# COMMAND ----------
