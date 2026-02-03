# Databricks notebook source

# COMMAND ----------

import numpy as np
import pandas as pd
import tqdm
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas

from reliability.Fitters import Fit_Weibull_2P_grouped, Fit_Weibull_2P, Fit_Everything
from reliability.Distributions import Weibull_Distribution


from datetime import datetime, timedelta, date

today_date = date.today()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view all_data as (
# MAGIC with all_devices as (
# MAGIC select l.*, datediff(last_heartbeat_date, first_heartbeat_date) as days_active, r.return_type
# MAGIC from hardware_analytics.gateways_life_summary as l
# MAGIC left join hardware.gateways_return_summary as r
# MAGIC on r.serial = l.serial
# MAGIC where (r.sam_number is null or array_contains(l.sam_numbers, r.sam_number))
# MAGIC and (l.return_date = r.date or l.return_date = r.returned_date or r.date is null)
# MAGIC ),
# MAGIC
# MAGIC final_data as (
# MAGIC select name, serial, sam_numbers, ship_date, first_heartbeat_date, last_heartbeat_date, return_date, return_type,
# MAGIC       case when return_date is not null and first_heartbeat_date is null and last_heartbeat_date is null then 0 else days_active end as days_active
# MAGIC from all_devices)
# MAGIC
# MAGIC select * from final_data
# MAGIC where days_active is not null
# MAGIC );
# MAGIC
# MAGIC cache table all_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select name, month(first_heartbeat_date), year(first_heartbeat_date), count(*)
# MAGIC from all_data
# MAGIC group by name, month(first_heartbeat_date), year(first_heartbeat_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC select model, data_subset, r.product_id, dp.name, max(date), max_by(alpha, date), max_by(beta, date)
# MAGIC from hardware_analytics.reliability_parameters as r
# MAGIC join definitions.products as dp
# MAGIC on dp.product_id = r.product_id
# MAGIC where data_subset = 'warrex_only'
# MAGIC group by model, data_subset, r.product_id, dp.name

# COMMAND ----------

products = ["VG34", "VG54-NA", "CM31", "CM32", "AG26", "AG46P", "AG52", "AG46", "AG51"]
product_code = {
    "VG34": 24,
    "VG54-NA": 53,
    "CM31": 44,
    "CM32": 43,
    "AG26": 68,
    "AG46P": 84,
    "AG52": 125,
    "AG46": 62,
    "AG51": 124,
}

# COMMAND ----------

# MAGIC %md
# MAGIC #Warranty Exchanges

# COMMAND ----------

# For Warranty Exhange returns
for product_type in tqdm.tqdm(products):
    SpSql = spark.sql(
        f"SELECT serial, days_active as time from all_data where name == '{product_type}'"
    )

    df_failure = (
        SpSql.filter("return_type = 'Warranty Exchange' ").select("time").toPandas()
    )
    df_censored = (
        SpSql.filter("return_type is null or return_type <> 'Warranty Exchange' ")
        .select("time")
        .toPandas()
    )

    df_f = df_failure["time"]
    df_c = df_censored["time"]

    fit = Fit_Everything(
        failures=df_f.values,
        right_censored=df_c.values,
        exclude=["Weibull_Mixture", "Weibull_CR"],
    )

    df = pd.DataFrame.from_dict(
        {
            "date": [str(today_date)],
            "product_id": [product_code[product_type]],
            "data_subset": ["warrex_only"],
            "model": ["Weibull_2P"],
            "alpha": [fit.Weibull_2P_alpha],
            "beta": [fit.Weibull_2P_beta],
        }
    )
    sdf = spark.createDataFrame(df)
    sdf.write.insertInto("hardware_analytics.reliability_parameters")


# COMMAND ----------

# MAGIC %md
# MAGIC #All Returns

# COMMAND ----------

# For All returns
for product_type in tqdm.tqdm(products):
    SpSql = spark.sql(
        f"SELECT serial, days_active as time from all_data where name == '{product_type}'"
    )

    df_failure = SpSql.filter("return_date is not null").select("time").toPandas()
    df_censored = SpSql.filter("return_date is null").select("time").toPandas()

    df_f = df_failure["time"]
    df_c = df_censored["time"]

    fit = Fit_Everything(
        failures=df_f.values,
        right_censored=df_c.values,
        exclude=["Weibull_Mixture", "Weibull_CR"],
    )

    df = pd.DataFrame.from_dict(
        {
            "date": [str(today_date)],
            "product_id": [product_code[product_type]],
            "data_subset": ["all_returns"],
            "model": ["Weibull_2P"],
            "alpha": [fit.Weibull_2P_alpha],
            "beta": [fit.Weibull_2P_beta],
        }
    )
    sdf = spark.createDataFrame(df)
    sdf.write.insertInto("hardware_analytics.reliability_parameters")
