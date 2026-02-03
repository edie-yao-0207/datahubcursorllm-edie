# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_stills_inference_processor"

# COMMAND ----------

step_values = get_steps_widgets()
region_name = dbutils.widgets.get("region_name")
inference_processor = TripStillsInferenceProcessor(region_name)

# COMMAND ----------

mode, min_date = get_step_value("trip_still_inference_processor", step_values)
inference_processor.process_inference(mode=mode, min_date_for_update=min_date)


# COMMAND ----------

# kick off combining data
trigger_next_nb(
    "/backend/datascience/stop_sign/combined_data/region_gen_nbs/region_combine_data_gen",
    f"{region_name}_combine_data_gen",
    region_name,
    additional_nb_params=step_values,
)
