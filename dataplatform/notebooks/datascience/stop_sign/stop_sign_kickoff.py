# Databricks notebook source

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/utils

# COMMAND ----------

min_date = "2020-11-01"
steps_params = {
    "trip_still_prep_mode": "ignore",
    "telem_gen_intersections_mode": "ignore",
    "telem_gen_location_data_mode": "ignore",
    "telem_partition_telem_data_mode": "ignore",
    "telem_histograms_gen_mode": "ignore",
    "telem_inference_mode": "ignore",
    "agg_speed_gen_mode": "ignore",
    "osm_way_tag_gen_mode": "ignore",
    "trip_still_locations_mode": "ignore",
    "trip_stills_intersections_mode": "ignore",
    "trip_stills_gen_image_paths_mode": "ignore",
    "trip_stills_inference_mode": "ignore",
    "trip_still_inference_processor_mode": "ignore",
    "combine_data_mode": "overwrite",
}

# COMMAND ----------

regions = [
    #   "test_region",
    "us_continental_telemetry_first_quarter",
    "us_continental_telemetry_second_quarter_first",
    "us_continental_telemetry_second_quarter_second",
    "us_continental_telemetry_second_quarter_third",
    "us_continental_telemetry_second_quarter_fourth",
    "us_continental_telemetry_third_quarter",
    "us_continental_telemetry_fourth_quarter_first",
    "us_continental_telemetry_fourth_quarter_second",
    "us_continental_telemetry_fourth_quarter_third",
    "us_continental_telemetry_fourth_quarter_fourth",
]

# COMMAND ----------

base_params = steps_params
base_params["regions"] = ",".join(regions)

# COMMAND ----------

trigger_next_nb(
    "/backend/datascience/stop_sign/trip_stills/region_gen_nbs/region_trip_stills_prep",
    "trip_stills_prep",
    "all",
    additional_nb_params=base_params,
)
