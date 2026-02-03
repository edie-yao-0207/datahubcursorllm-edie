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

create or replace temp view filtered_devices as (
  select
    a.date,
    a.org_id,
    a.cm_device_id as device_id
  from
    dataprep_safety.cm_device_daily_metadata as a
    join dataprep_firmware.speeding_daily_active_devices as b on a.date = b.date
    and a.cm_device_id = b.device_id
    and a.org_id = b.org_id
  where
		a.date between ('2024-05-01') and ('2024-07-31')
		-- and ('all' in ({"customer"}) or lower(a.org_type) in ({"customer"}))
		-- and ('all' in ({{ VG firmware }}) or a.last_reported_vg_build in ({{ VG firmware }}))
		-- and ('all' in ({{ CM product name }}) or a.cm_product_name in ({{ CM product name }}))
		-- and ('all' in ({{ CM firmware }}) or a.last_reported_cm_build in ({{ CM firmware }}))
		-- and ('all' in ({{ CM product program ID }}) or a.cm_product_program_id in ({{ CM product program ID }}))
		-- and ('all' in ({{ CM rollout stage ID }}) or a.cm_rollout_stage_id in ({{ CM rollout stage ID }}))
        and weekday(a.date) < 5 and weekday(a.date) > 0
);
create or replace temp view speed_limit_count_per_device as (
  select
    b.date,
    b.org_id,
    b.device_id,
    coalesce(sum(a.count), 0) as count
  from
    dataprep_firmware.speeding_speed_limit_count as a
    right join filtered_devices as b
      on a.date = b.date
      and a.org_id = b.org_id
      and a.device_id = b.device_id
  group by
    b.date,
    b.org_id,
    b.device_id
);
create or replace temp view speed_limit_count_metric as (
    select
    date,
    percentile_approx(count, 0.01) as p1,
    percentile_approx(count, 0.025) as p025,
    percentile_approx(count, 0.035) as p035,
    percentile_approx(count, 0.05) as p05,
    percentile_approx(count, 0.075) as p075,
    percentile_approx(count, 0.1) as p10,
    percentile_approx(count, 0.15) as p15,
    percentile_approx(count, 0.5) as median,
    percentile_approx(count, 0.90) as p90,
    percentile_approx(count, 0.99) as p99,
    avg(count) as avg
    from
    speed_limit_count_per_device
    group by
    date
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC speed_limit_count = spark.sql('select * from speed_limit_count_metric').toPandas()

-- COMMAND ----------

select * from speed_limit_count_metric

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## OTHER DATA POINTS

-- COMMAND ----------

-- MAGIC %python
-- MAGIC season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(speed_limit_count,time_column='date',value_column='p05',frequency='d',data_type='continuous',task={'trend':'BOCPD'},zoom_in_point='2024-06-28',figsize=[18,3], zoom_in_before=60)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime, timedelta
-- MAGIC
-- MAGIC current_date = datetime.strptime('2024-06-27', "%Y-%m-%d")
-- MAGIC end_date = datetime.strptime('2024-06-28', "%Y-%m-%d")
-- MAGIC
-- MAGIC current_date += timedelta(days=1)
-- MAGIC
-- MAGIC anomalous_dates = []
-- MAGIC
-- MAGIC while current_date <= end_date:
-- MAGIC     season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date= detect(speed_limit_count,time_column='date',value_column='p05',frequency='d',data_type='continuous',task={'trend':'BOCPD'},zoom_in_point=current_date,figsize=[18,3], zoom_in_before=60, zoom_in_after=2)
-- MAGIC     if (current_date in data_with_date[change_x]):
-- MAGIC         anomalous_dates.append(current_date)
-- MAGIC         print("added: ")
-- MAGIC     current_date += timedelta(days=1)
-- MAGIC
-- MAGIC print(anomalous_dates)
