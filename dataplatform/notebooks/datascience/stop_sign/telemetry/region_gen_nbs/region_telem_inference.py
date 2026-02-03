# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/telemetry/region_telemetry_inference"

# COMMAND ----------

region_name = dbutils.widgets.get("region_name")
step_values = get_steps_widgets()

# COMMAND ----------

region_telemetry_inference = RegionTelemetryInference(region_name)

# COMMAND ----------

mode, min_date = get_step_value("telem_inference", step_values)
region_telemetry_inference.run_and_save(mode=mode)

# COMMAND ----------
