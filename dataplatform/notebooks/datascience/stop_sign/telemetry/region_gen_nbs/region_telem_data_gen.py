# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run backend/datascience/stop_sign/telemetry/region_generator

# COMMAND ----------

step_values = get_steps_widgets()

# COMMAND ----------

region_name = dbutils.widgets.get("region_name")
row_df = spark.table("stopsigns.region_bounds").filter(
    F.col("region_name") == region_name
)
nw_lat = row_df.select("nw_lat").rdd.flatMap(lambda x: x).collect()[0]
nw_lon = row_df.select("nw_lon").rdd.flatMap(lambda x: x).collect()[0]
se_lat = row_df.select("se_lat").rdd.flatMap(lambda x: x).collect()[0]
se_lon = row_df.select("se_lon").rdd.flatMap(lambda x: x).collect()[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate City Data

# COMMAND ----------

region_generator = RegionTelemetryGenerator(nw_lat, nw_lon, se_lat, se_lon, region_name)

# COMMAND ----------

# generate intersections
mode, min_date = get_step_value("telem_gen_intersections", step_values)
region_generator.generate_intersections(mode=mode)

# COMMAND ----------

# generate location data
mode, min_date = get_step_value("telem_gen_location_data", step_values)
region_generator.generate_location_data(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

# kick off trip still location nb now that location data is generated
trigger_next_nb(
    "/backend/datascience/stop_sign/trip_stills/region_gen_nbs/region_trip_still_locations_gen",
    f"{region_name}_trip_still_loc_gen",
    region_name,
    additional_nb_params=step_values,
)

# COMMAND ----------

mode, min_date = get_step_value("telem_partition_telem_data", step_values)
region_generator.generate_telemetry_data(mode=mode, min_date_for_update=min_date)

# COMMAND ----------

mode, min_date = get_step_value("telem_histograms_gen", step_values)
region_generator.generate_histograms(mode=mode)

# COMMAND ----------

mode, min_date = get_step_value("telem_inference", step_values)
region_generator.create_inference_table(mode=mode)

# COMMAND ----------

mode, min_date = get_step_value("agg_speed_gen", step_values)
region_generator.create_agg_speed_table(mode=mode)

mode, min_date = get_step_value("osm_way_tag_gen", step_values)
region_generator.create_osm_way_tag_table(mode=mode)

# COMMAND ----------

# kick off inference
trigger_next_nb(
    "/backend/datascience/stop_sign/telemetry/region_gen_nbs/region_telem_inference",
    f"{region_name}_telem_inference_gen",
    region_name,
    cluster_config_type="telemetry_inference",
    additional_nb_params=step_values,
)
