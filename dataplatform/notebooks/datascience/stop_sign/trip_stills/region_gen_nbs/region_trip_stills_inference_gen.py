# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_inference"

# COMMAND ----------

step_values = get_steps_widgets()
region_name = dbutils.widgets.get("region_name")
trip_still_inference = TripStillInference(region_name)

# COMMAND ----------

mode, min_date = get_step_value("trip_stills_inference", step_values)
trip_still_inference.run_inference(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

# kick off inference processsing
trigger_next_nb(
    "/backend/datascience/stop_sign/trip_stills/region_gen_nbs/region_trip_still_inference_processor_gen",
    f"{region_name}_trip_stills_inference_processor_gen",
    region_name,
    additional_nb_params=step_values,
)
