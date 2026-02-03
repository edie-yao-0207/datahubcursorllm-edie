# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/telemetry/region_telemetry_inference"

# COMMAND ----------

region_telemetry_inference = RegionTelemetryInference(
    "us_continental_telemetry_fourth_quarter_fourth"
)

# COMMAND ----------

indices_to_exclude = []
region_telemetry_inference.run_and_save(indices_to_exclude=indices_to_exclude)

# COMMAND ----------
