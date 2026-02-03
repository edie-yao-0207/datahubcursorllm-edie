-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Imports

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pip install cvxopt

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %pip install -U csaps
-- MAGIC %pip install ruptures
-- MAGIC %pip install statsmodels==0.14.0
-- MAGIC %pip install scipy==1.11.1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from __future__ import division
-- MAGIC import numpy as np
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC from statsmodels.tsa.seasonal import seasonal_decompose, STL, MSTL
-- MAGIC from scipy.interpolate import splrep, BSpline
-- MAGIC from csaps import csaps
-- MAGIC import datetime
-- MAGIC import matplotlib_inline.backend_inline
-- MAGIC
-- MAGIC %matplotlib inline
-- MAGIC %load_ext autoreload
-- MAGIC %autoreload 2
-- MAGIC
-- MAGIC import matplotlib_inline.backend_inline
-- MAGIC matplotlib_inline.backend_inline.set_matplotlib_formats('png2x')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Library

-- COMMAND ----------

-- MAGIC %run ../data_analysis_library
-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Get Data from Databases

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sorted_data AS (
    SELECT
        device_id,
        CAST(date AS DATE) AS date,
        last_reported_cm_build,
        LAG(last_reported_cm_build) OVER (PARTITION BY device_id ORDER BY CAST(date AS DATE)) AS previous_cm_build
    FROM
        hive_metastore.dataprep_safety.cm_device_health_daily
    WHERE
        last_reported_cm_build IS NOT NULL
);

CREATE OR REPLACE TEMP VIEW processed_data AS (
    SELECT
        device_id,
        date,
        last_reported_cm_build,
        previous_cm_build,
        CASE
            WHEN last_reported_cm_build != previous_cm_build THEN 'changed'
            ELSE 'unchanged'
        END AS cm_build_status
    FROM
        sorted_data
);

CREATE OR REPLACE TEMP VIEW counted_data AS (
    SELECT
        date,
        COUNT(*) AS change_count
    FROM
        processed_data
    WHERE
        cm_build_status = 'changed'
    GROUP BY
        date
    ORDER BY
        date
);

CREATE OR REPLACE TEMP VIEW data_with_lag_lead AS (
    SELECT
        date,
        change_count,
        LAG(change_count, 1) OVER (ORDER BY date) AS previous_value,
        LEAD(change_count, 1) OVER (ORDER BY date) AS next_value
    FROM
        counted_data
);

CREATE OR REPLACE TEMP VIEW rolling_max_data AS (
    SELECT
        date,
        change_count,
        MAX(change_count) OVER (
            ORDER BY date
            ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
        ) AS rolling_max_count
    FROM
        data_with_lag_lead
);

CREATE OR REPLACE TEMP VIEW firmware_rollout_dates AS (
    SELECT
        date,
        change_count
    FROM
        rolling_max_data
    WHERE
        change_count = rolling_max_count
        -- AND change_count > 45000
    ORDER BY
        date
);


-- COMMAND ----------

-- MAGIC %python
-- MAGIC firmware_rollouts = spark.sql('select date from firmware_rollout_dates').toPandas().date.to_list()
-- MAGIC firmware_rollouts = pd.to_datetime(firmware_rollouts)

-- COMMAND ----------

create or replace temp view hwwatchdog_affected_devices as (
  select device_id
  from dataprep_firmware.gateway_daily_rollout_stages
  where product_program_id = 0
  and rollout_stage_id in (1000)
  and date = '2024-04-10'
  and product_id in (43, 44) -- added this
);

CREATE OR REPLACE TEMP VIEW hwatchdog AS (
  SELECT
    a.date,
    a.device_id,
    SUM(a.num_anomalies) AS num_anomalies,
    SUM(a.trip_length_hours) AS trip_length_hours
  FROM hive_metastore.firmware_dev.eli_hwwatchdog_anomalies_on_trip_all_time as a JOIN hwwatchdog_affected_devices as b
  ON a.device_id = b.device_id
  WHERE a.date > '2024-01-01'
  GROUP BY a.device_id, a.date
  ORDER BY a.date, a.device_id
);

CREATE OR REPLACE TEMP VIEW hwatchdog_table AS (
  SELECT
    date,
    num_anomalies / trip_length_hours * 100 AS hwatchdog
  FROM hwatchdog
);

CREATE OR REPLACE TEMP VIEW hwatchdog_metrics AS (
  SELECT
    date,
    median(hwatchdog) AS hwatchdog_median,
    avg(hwatchdog) AS hwatchdog_avg,
    percentile_approx(hwatchdog, 0.1) AS hwatchdog_p_10,
    percentile_approx(hwatchdog, 0.2) AS hwatchdog_p_20,
    percentile_approx(hwatchdog, 0.3) AS hwatchdog_p_30,
    percentile_approx(hwatchdog, 0.4) AS hwatchdog_p_40,
    percentile_approx(hwatchdog, 0.5) AS hwatchdog_p_50,
    percentile_approx(hwatchdog, 0.6) AS hwatchdog_p_60,
    percentile_approx(hwatchdog, 0.7) AS hwatchdog_p_70,
    percentile_approx(hwatchdog, 0.8) AS hwatchdog_p_80,
    percentile_approx(hwatchdog, 0.90) AS hwatchdog_p_90,
    percentile_approx(hwatchdog, 0.95) AS hwatchdog_p_95,
    percentile_approx(hwatchdog, 0.96) AS hwatchdog_p_96,
    percentile_approx(hwatchdog, 0.97) AS hwatchdog_p_97,
    percentile_approx(hwatchdog, 0.98) AS hwatchdog_p_98,
    percentile_approx(hwatchdog, 0.99) AS hwatchdog_p_99
  FROM
    hwatchdog_table
  GROUP BY
    date
  ORDER BY
    date DESC
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC hwatchdog = spark.sql('select * from hwatchdog_metrics').toPandas()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OTHER DATA POINTS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(hwatchdog,time_column='date',value_column='hwatchdog_avg',frequency='d',data_type='continuous',task={'season':'MSTL', 'trend':'BOCPD'},zoom_in_point='2024-03-23',figsize=[18,3], periods=[7], significance_level=0.05)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC current_date = datetime.strptime('2024-03-01', "%Y-%m-%d")
-- MAGIC end_date = datetime.strptime('2024-04-15', "%Y-%m-%d")
-- MAGIC
-- MAGIC current_date += timedelta(days=1)
-- MAGIC
-- MAGIC anomalous_dates = []
-- MAGIC
-- MAGIC while current_date <= end_date:
-- MAGIC     season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(hwatchdog,time_column='date',value_column='hwatchdog_avg',frequency='d',data_type='continuous',task={'season':'MSTL', 'trend':'BOCPD'},zoom_in_point=current_date,figsize=[18,3], periods=[7])
-- MAGIC     if (current_date in data_with_date[change_x]):
-- MAGIC         anomalous_dates.append(current_date)
-- MAGIC         print("added: ")
-- MAGIC     print(current_date)
-- MAGIC     current_date += timedelta(days=1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(anomalous_dates)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC current_date = datetime.strptime('2024-03-01', "%Y-%m-%d")
-- MAGIC end_date = datetime.strptime('2024-04-15', "%Y-%m-%d")
-- MAGIC
-- MAGIC current_date += timedelta(days=1)
-- MAGIC
-- MAGIC anomalous_dates = []
-- MAGIC
-- MAGIC while current_date <= end_date:
-- MAGIC     season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(hwatchdog,time_column='date',value_column='hwatchdog_avg',frequency='d',data_type='continuous',task={'trend':'BOCPD'},zoom_in_point=current_date,figsize=[18,3], zoom_in_after=2)
-- MAGIC     if (current_date in data_with_date[change_x]):
-- MAGIC         anomalous_dates.append(current_date)
-- MAGIC         print("added: ")
-- MAGIC     print(current_date)
-- MAGIC     current_date += timedelta(days=1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(anomalous_dates)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(hwatchdog,time_column='date',value_column='hwatchdog_avg',frequency='d',data_type='continuous',task={'trend':'BOCPD'},zoom_in_point='2024-03-23',figsize=[18,3], zoom_in_after=2)
