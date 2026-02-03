# Databricks notebook source
# Following install commands were commented out as they are no longer
# supported in UC clusters. Please ensure that this notebook is run
# in a cluster with the required libraries installed.
# dbutils.library.installPyPI("contextlib2")
# dbutils.library.installPyPI("scikit-image")

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/trip_stills/trip_still_intersections"

# COMMAND ----------

trip_stills_intersections = TripStillIntersections(
    "us_continental_telemetry_third_quarter"
)

# COMMAND ----------

trip_stills_intersections.assign_to_ints()

# COMMAND ----------

trip_stills_intersections.gen_image_paths_df(indices_to_exclude=[])

# COMMAND ----------
