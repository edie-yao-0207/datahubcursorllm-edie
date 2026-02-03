# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_loc_gen"


# COMMAND ----------

step_values = get_steps_widgets()
region_name = dbutils.widgets.get("region_name")

# COMMAND ----------

region_gen = TripStillLocGen(region_name)

# COMMAND ----------

mode, min_date = get_step_value("trip_still_locations", step_values)
region_gen.run_and_save(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

# kick off trip still intersections
trigger_next_nb(
    "/backend/datascience/stop_sign/trip_stills/region_gen_nbs/region_trip_stills_intersections_gen",
    f"{region_name}_trip_stills_intersection_gen",
    region_name,
    additional_nb_params=step_values,
)
