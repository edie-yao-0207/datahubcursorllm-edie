# Databricks notebook source
# MAGIC %run "backend/datascience/stop_sign/combined_data/combine_data_class"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/model/gbdt_stopsign_model"

# COMMAND ----------

# MAGIC %run "backend/datascience/stop_sign/model/model_config"

# COMMAND ----------

model_config = ModelConfig()
gbdt_model = GBDTStopSignModel(model_config.MODEL_DIR)
step_values = get_steps_widgets()
region_name = dbutils.widgets.get("region_name")
combiner = CombineData(region_name, gbdt_model)

# COMMAND ----------

mode, min_date = get_step_value("combine_data", step_values)
combiner.combine(ground_truth_cities=["sf", "seattle", "la"], mode=mode)

# COMMAND ----------

combiner.union_to_one_table()
