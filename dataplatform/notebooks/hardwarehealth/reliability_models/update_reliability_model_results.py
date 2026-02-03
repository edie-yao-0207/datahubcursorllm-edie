# Databricks notebook source
# MAGIC %sh
# MAGIC pip uninstall -y numpy
# MAGIC pip uninstall -y setuptools
# MAGIC pip install setuptools
# MAGIC pip install numpy
# MAGIC pip install reliability
# MAGIC pip install pyspark

# COMMAND ----------

# MAGIC %sh pip install pyspark==3.4

# COMMAND ----------

import numpy as np
import pandas as pd
from reliability.Distributions import Weibull_Distribution


# COMMAND ----------

# MAGIC %sql select * from hardware_analytics.reliability_parameters

# COMMAND ----------

df = spark.sql(
    """select product_id, data_subset, max(date) as date, max_by(alpha, date) as alpha, max_by(beta, date) as beta
from hardware_analytics.reliability_parameters
where data_subset in ('warrex_only', 'all_returns')
group by product_id, data_subset"""
).toPandas()
df

# COMMAND ----------

years = np.arange(0, 11, 1) * 365
results = []
for index, row in df.iterrows():
    fit = Weibull_Distribution(alpha=row["alpha"], beta=row["beta"])
    perc_survive = fit.SF(xvals=years, show_plot=False, plot_CI=False) * 100
    rdf = pd.DataFrame({"years": years, "surviving_percent": perc_survive})
    rdf["product_id"] = row["product_id"]
    rdf["type"] = row["data_subset"]
    rdf = rdf[["product_id", "type", "years", "surviving_percent"]]
    results.append(rdf)

results = pd.concat(results)
results.reset_index(inplace=True, drop=True)
results

# COMMAND ----------

import pyspark

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists hardware.reliability_measures

# COMMAND ----------

sdf = spark.createDataFrame(results)
sdf.write.format("delta").saveAsTable("hardware.reliability_measures")

# COMMAND ----------
