# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/combined_data/combine_data_class"

# COMMAND ----------

combiner = CombineData("us_continental_telemetry_second_quarter_second")

# COMMAND ----------

indices_to_exclude = []
combiner.combine(
    ground_truth_cities=["sf", "seattle", "la"], indices_to_exclude=indices_to_exclude
)

# COMMAND ----------

combiner.union_to_one_table(indices_to_exclude=indices_to_exclude)

# COMMAND ----------
