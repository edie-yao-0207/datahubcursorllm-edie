# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_intersections"

# COMMAND ----------

step_values = get_steps_widgets()
region_name = dbutils.widgets.get("region_name")

# COMMAND ----------

trip_stills_intersections = TripStillIntersections(region_name)

# COMMAND ----------

mode, min_date = get_step_value("trip_stills_intersections", step_values)
trip_stills_intersections.assign_to_ints(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

mode, min_date = get_step_value("trip_stills_gen_image_paths", step_values)
trip_stills_intersections.gen_image_paths_df(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

# kick off inference
trigger_next_nb(
    "/backend/datascience/stop_sign/trip_stills/region_gen_nbs/region_trip_stills_inference_gen",
    f"{region_name}_trip_stills_inference_gen",
    region_name,
    cluster_config_type="trip_still_inference",
    additional_nb_params=step_values,
)
