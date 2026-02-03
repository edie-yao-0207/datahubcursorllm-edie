# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Imports
# MAGIC %pip install cvxopt
# MAGIC %pip install -U csaps
# MAGIC %pip install ruptures
# MAGIC %pip install statsmodels==0.14.0
# MAGIC %pip install scipy==1.11.1

# COMMAND ----------

from __future__ import division
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose, STL, MSTL
from scipy.interpolate import splrep, BSpline
from csaps import csaps
import datetime
import matplotlib_inline.backend_inline

matplotlib_inline.backend_inline.set_matplotlib_formats("png2x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Library

# COMMAND ----------

# MAGIC %run ../data_analysis_library

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Data from Databases

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view sorted_data AS (
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         date,
# MAGIC         last_reported_cm_build,
# MAGIC         LAG(last_reported_cm_build) OVER (PARTITION BY device_id ORDER BY date) AS previous_cm_build
# MAGIC     FROM
# MAGIC         hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC     WHERE
# MAGIC         last_reported_cm_build IS NOT NULL
# MAGIC );
# MAGIC create or replace temp view processed_data AS (
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         date,
# MAGIC         last_reported_cm_build,
# MAGIC         previous_cm_build,
# MAGIC         CASE
# MAGIC             WHEN last_reported_cm_build != previous_cm_build THEN 'changed'
# MAGIC             ELSE 'unchanged'
# MAGIC         END AS cm_build_status
# MAGIC     FROM
# MAGIC         sorted_data
# MAGIC );
# MAGIC create or replace temp view counted_data AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         COUNT(*) AS change_count
# MAGIC     FROM
# MAGIC         processed_data
# MAGIC     WHERE
# MAGIC         cm_build_status = 'changed'
# MAGIC     GROUP BY
# MAGIC         date
# MAGIC     ORDER BY
# MAGIC         date
# MAGIC );
# MAGIC create or replace temp view data_with_lag_lead AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         change_count,
# MAGIC         LAG(change_count, 1) OVER (ORDER BY date) AS previous_value,
# MAGIC         LEAD(change_count, 1) OVER (ORDER BY date) AS next_value
# MAGIC     FROM
# MAGIC         counted_data
# MAGIC );
# MAGIC create or replace temp view firmware_rollout_dates AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         change_count
# MAGIC     FROM
# MAGIC         data_with_lag_lead
# MAGIC     WHERE
# MAGIC         change_count > previous_value AND change_count > next_value
# MAGIC         AND change_count > 45000
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sorted_data AS (
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         CAST(da te AS DATE) AS date,
# MAGIC         last_reported_cm_build,
# MAGIC         LAG(last_reported_cm_build) OVER (PARTITION BY device_id ORDER BY CAST(date AS DATE)) AS previous_cm_build
# MAGIC     FROM
# MAGIC         hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC     WHERE
# MAGIC         last_reported_cm_build IS NOT NULL
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW processed_data AS (
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         date,
# MAGIC         last_reported_cm_build,
# MAGIC         previous_cm_build,
# MAGIC         CASE
# MAGIC             WHEN last_reported_cm_build != previous_cm_build THEN 'changed'
# MAGIC             ELSE 'unchanged'
# MAGIC         END AS cm_build_status
# MAGIC     FROM
# MAGIC         sorted_data
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW counted_data AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         COUNT(*) AS change_count
# MAGIC     FROM
# MAGIC         processed_data
# MAGIC     WHERE
# MAGIC         cm_build_status = 'changed'
# MAGIC     GROUP BY
# MAGIC         date
# MAGIC     ORDER BY
# MAGIC         date
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW data_with_lag_lead AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         change_count,
# MAGIC         LAG(change_count, 1) OVER (ORDER BY date) AS previous_value,
# MAGIC         LEAD(change_count, 1) OVER (ORDER BY date) AS next_value
# MAGIC     FROM
# MAGIC         counted_data
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rolling_max_data AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         change_count,
# MAGIC         MAX(change_count) OVER (
# MAGIC             ORDER BY date
# MAGIC             ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING
# MAGIC         ) AS rolling_max_count
# MAGIC     FROM
# MAGIC         data_with_lag_lead
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW firmware_rollout_dates AS (
# MAGIC     SELECT
# MAGIC         date,
# MAGIC         change_count
# MAGIC     FROM
# MAGIC         rolling_max_data
# MAGIC     WHERE
# MAGIC         change_count = rolling_max_count
# MAGIC         AND change_count > 45000
# MAGIC     ORDER BY
# MAGIC         date
# MAGIC );
# MAGIC
# MAGIC -- To get the final result, select from firmware_rollout_dates
# MAGIC SELECT * FROM firmware_rollout_dates;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from firmware_rollout_dates

# COMMAND ----------

firmware_rollouts = (
    spark.sql("select date from firmware_rollout_dates").toPandas().date.to_list()
)
firmware_rollouts = pd.to_datetime(firmware_rollouts)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view heat_data as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     device_id,
# MAGIC     SUM(total_safe_mode_duration_ms) AS total_safe_mode_duration_ms,
# MAGIC     SUM(total_overheated_duration_ms) AS total_overheated_duration_ms,
# MAGIC     SUM(total_trip_duration_ms) AS total_trip_duration_ms
# MAGIC   FROM hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC   WHERE date < '2021-08-01' and date > '2021-05-01'
# MAGIC   GROUP BY device_id, date
# MAGIC   ORDER BY date, device_id
# MAGIC );
# MAGIC create or replace temp view ai_downtime_table as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     100 * (total_safe_mode_duration_ms + total_overheated_duration_ms) / total_trip_duration_ms as ai_downtime
# MAGIC   FROM heat_data
# MAGIC );
# MAGIC create or replace temp view ai_downtime_metrics as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     median(ai_downtime) as ai_downtime_median,
# MAGIC     avg(ai_downtime) as ai_downtime_avg,
# MAGIC     percentile_approx(ai_downtime, 0.1) AS ai_downtime_p_10,
# MAGIC     percentile_approx(ai_downtime, 0.2) AS ai_downtime_p_20,
# MAGIC     percentile_approx(ai_downtime, 0.3) AS ai_downtime_p_30,
# MAGIC     percentile_approx(ai_downtime, 0.4) AS ai_downtime_p_40,
# MAGIC     percentile_approx(ai_downtime, 0.5) AS ai_downtime_p_50,
# MAGIC     percentile_approx(ai_downtime, 0.6) AS ai_downtime_p_60,
# MAGIC     percentile_approx(ai_downtime, 0.7) AS ai_downtime_p_70,
# MAGIC     percentile_approx(ai_downtime, 0.8) AS ai_downtime_p_80,
# MAGIC     percentile_approx(ai_downtime, 0.90) AS ai_downtime_p_90,
# MAGIC     percentile_approx(ai_downtime, 0.95) AS ai_downtime_p_95,
# MAGIC     percentile_approx(ai_downtime, 0.96) AS ai_downtime_p_96,
# MAGIC     percentile_approx(ai_downtime, 0.97) AS ai_downtime_p_97,
# MAGIC     percentile_approx(ai_downtime, 0.98) AS ai_downtime_p_98,
# MAGIC     percentile_approx(ai_downtime, 0.99) AS ai_downtime_p_99
# MAGIC   FROM
# MAGIC     ai_downtime_table
# MAGIC   GROUP BY
# MAGIC     date
# MAGIC   ORDER BY
# MAGIC     date DESC
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ai_downtime_metrics;

# COMMAND ----------

ai_downtime = spark.sql("select * from ai_downtime_metrics").toPandas()

# COMMAND ----------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW hwatchdog AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     device_id,
# MAGIC     SUM(num_anomalies) AS num_anomalies,
# MAGIC     SUM(trip_length_hours) AS trip_length_hours
# MAGIC   FROM hive_metastore.firmware_dev.eli_hwwatchdog_anomalies_on_trip_all_time
# MAGIC   GROUP BY device_id, date
# MAGIC   ORDER BY date, device_id
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW hwatchdog_table AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     num_anomalies / trip_length_hours AS hwatchdog
# MAGIC   FROM hwatchdog
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW hwatchdog_metrics AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     median(hwatchdog) AS hwatchdog_median,
# MAGIC     avg(hwatchdog) AS hwatchdog_avg,
# MAGIC     percentile_approx(hwatchdog, 0.1) AS hwatchdog_p_10,
# MAGIC     percentile_approx(hwatchdog, 0.2) AS hwatchdog_p_20,
# MAGIC     percentile_approx(hwatchdog, 0.3) AS hwatchdog_p_30,
# MAGIC     percentile_approx(hwatchdog, 0.4) AS hwatchdog_p_40,
# MAGIC     percentile_approx(hwatchdog, 0.5) AS hwatchdog_p_50,
# MAGIC     percentile_approx(hwatchdog, 0.6) AS hwatchdog_p_60,
# MAGIC     percentile_approx(hwatchdog, 0.7) AS hwatchdog_p_70,
# MAGIC     percentile_approx(hwatchdog, 0.8) AS hwatchdog_p_80,
# MAGIC     percentile_approx(hwatchdog, 0.90) AS hwatchdog_p_90,
# MAGIC     percentile_approx(hwatchdog, 0.95) AS hwatchdog_p_95,
# MAGIC     percentile_approx(hwatchdog, 0.96) AS hwatchdog_p_96,
# MAGIC     percentile_approx(hwatchdog, 0.97) AS hwatchdog_p_97,
# MAGIC     percentile_approx(hwatchdog, 0.98) AS hwatchdog_p_98,
# MAGIC     percentile_approx(hwatchdog, 0.99) AS hwatchdog_p_99
# MAGIC   FROM
# MAGIC     hwatchdog_table
# MAGIC   GROUP BY
# MAGIC     date
# MAGIC   ORDER BY
# MAGIC     date DESC
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hwatchdog_metrics

# COMMAND ----------

hwatchdog = spark.sql("select * from hwatchdog_metrics").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic testing (trying functions on data collected)

# COMMAND ----------

# DBTITLE 1,AI Downtime
(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_avg",
    frequency="d",
    data_type="continuous",
    task={"season": "MSTL", "trend": "Dynp", "outlier": "spline_isoForest"},
    periods=[7, 28, 365],
)

# COMMAND ----------

# DBTITLE 1,AI Downtime
import matplotlib.pyplot as plt
import numpy as np

y = np.array(data_with_date.to_list())
y = y.reshape(
    -1,
)
x = data_with_date.index

fig, ax = plt.subplots(figsize=[18, 3])
ax.plot(x, y)
ax.scatter(
    data_with_date.index[outlier_x], y[outlier_x], color="red", label="outlier points"
)
ax.scatter(
    data_with_date.index[change_x],
    y[change_x],
    color="blue",
    label="trend segmentation point",
)
ax.set_title("Original Data and Change Points")

x_min, x_max = x.min(), x.max()

for firmware_rollout in firmware_rollouts.date:
    if x_min <= firmware_rollout <= x_max:
        ax.axvline(x=firmware_rollout, color="black", linestyle="--")


ax.legend()

# COMMAND ----------

(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_avg",
    frequency="d",
    data_type="continuous",
    task={"trend": "BOCPD"},
    zoom_in_point="07/18/2021",
    figsize=[18, 3],
)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

y = np.array(data_with_date.to_list())
y = y.reshape(
    -1,
)
x = data_with_date.index

# print(data_with_date.index[change_x[1]])

fig, ax = plt.subplots(figsize=[18, 3])
ax.plot(x, y)
# ax.scatter(data_with_date.index[outlier_x],y[outlier_x],color='red',label='outlier points')
ax.scatter(
    data_with_date.index[change_x[0]],
    y[change_x[0]],
    color="blue",
    label="trend segmentation point",
)
ax.text(
    data_with_date.index[change_x[0]],
    y[change_x[0]],
    "   2021-07-16",
    color="red",
    ha="left",
)

x_min, x_max = x.min(), x.max()

# for firmware_rollout in firmware_rollouts.date:
#     if x_min <= firmware_rollout <= x_max:
#         ax.axvline(x=firmware_rollout, color='black', linestyle='--')

# Set the title and labels for the plot
ax.set_title("Trend Changes in Avg AI Downtime per Device (%)")
ax.set_xlabel("Date")
ax.set_ylabel("Avg AI Downtime per Device (%)")

ax.legend()

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

y = np.array(data_with_date.to_list())
y = y.reshape(
    -1,
)
x = data_with_date.index

fig, ax = plt.subplots(figsize=[18, 3])
ax.plot(x, y)
ax.scatter(
    data_with_date.index[outlier_x], y[outlier_x], color="red", label="outlier points"
)
ax.scatter(
    data_with_date.index[change_x],
    y[change_x],
    color="blue",
    label="trend segmentation point",
)
ax.set_title("Original Data and Change Points")

x_min, x_max = x.min(), x.max()

for firmware_rollout in firmware_rollouts.date:
    if x_min <= firmware_rollout <= x_max:
        ax.axvline(x=firmware_rollout, color="black", linestyle="--")

ax.legend()

# COMMAND ----------

(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_avg",
    frequency="d",
    data_type="continuous",
    task={"trend": "BOCPD"},
    zoom_in_point="09/23/2021",
    figsize=[18, 3],
)

# COMMAND ----------

from datetime import datetime, timedelta

end_date = datetime.strptime(ai_downtime.date[0], "%Y-%m-%d")
current_date = datetime.strptime(
    ai_downtime.date[ai_downtime.date.size - 1], "%Y-%m-%d"
)

current_date += timedelta(days=1)

anomalous_dates = []

while current_date <= end_date:
    (
        season,
        trend,
        resid,
        deseason,
        outlier_x,
        outlier_y,
        impute_data,
        change_x,
        change_y,
        detected_lag,
        data_with_date,
    ) = detect(
        ai_downtime,
        time_column="date",
        value_column="ai_downtime_avg",
        frequency="d",
        data_type="continuous",
        task={"trend": "BOCPD"},
        zoom_in_point=current_date.strftime("%Y-%m-%d"),
        figsize=[18, 3],
    )
    if current_date in data_with_date[change_x]:
        anomalous_dates.append(current_date)
        print("added: ")
    print(current_date)
    current_date += timedelta(days=1)

# COMMAND ----------

# DBTITLE 1,AI Downtime removing weekly seasonality
(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
    _,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_avg",
    frequency="d",
    data_type="continuous",
    task={"season": "MSTL", "trend": "BOCPD"},
    zoom_in_point="07/18/2021",
    figsize=[18, 3],
    periods=[7],
)  # significance_level=0.05

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

y = np.array(data_with_date.to_list())
y = y.reshape(
    -1,
)
x = data_with_date.index

fig, ax = plt.subplots(figsize=[18, 3])
ax.plot(x, y)
ax.scatter(
    data_with_date.index[outlier_x], y[outlier_x], color="red", label="outlier points"
)
ax.scatter(
    data_with_date.index[change_x],
    y[change_x],
    color="blue",
    label="trend segmentation point",
)
ax.set_title("Original Data and Change Points")

x_min, x_max = x.min(), x.max()

for firmware_rollout in firmware_rollouts.date:
    if x_min <= firmware_rollout <= x_max:
        ax.axvline(x=firmware_rollout, color="black", linestyle="--")

ax.legend()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing on changes in averages
# MAGIC
# MAGIC Not as effective in finding anomalies as testing on full suite (percentiles, average, median)

# COMMAND ----------

# DBTITLE 1,TEST NEGATIVES
# MAGIC %sql
# MAGIC create or replace temp view heat_data as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     device_id,
# MAGIC     SUM(total_safe_mode_duration_ms) AS total_safe_mode_duration_ms,
# MAGIC     SUM(total_overheated_duration_ms) AS total_overheated_duration_ms,
# MAGIC     SUM(total_trip_duration_ms) AS total_trip_duration_ms
# MAGIC   FROM hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC   WHERE date > '2021-11-01'
# MAGIC     -- AND rand() <= 0.01
# MAGIC   GROUP BY device_id, date
# MAGIC   ORDER BY date, device_id
# MAGIC );
# MAGIC create or replace temp view ai_downtime_table as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     100 * (total_safe_mode_duration_ms + total_overheated_duration_ms) / total_trip_duration_ms as ai_downtime
# MAGIC   FROM heat_data
# MAGIC );
# MAGIC create or replace temp view ai_downtime_metrics as (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     median(ai_downtime) as ai_downtime_median,
# MAGIC     avg(ai_downtime) as ai_downtime_avg,
# MAGIC     percentile_approx(ai_downtime, 0.1) AS ai_downtime_p_10,
# MAGIC     percentile_approx(ai_downtime, 0.2) AS ai_downtime_p_20,
# MAGIC     percentile_approx(ai_downtime, 0.3) AS ai_downtime_p_30,
# MAGIC     percentile_approx(ai_downtime, 0.4) AS ai_downtime_p_40,
# MAGIC     percentile_approx(ai_downtime, 0.5) AS ai_downtime_p_50,
# MAGIC     percentile_approx(ai_downtime, 0.6) AS ai_downtime_p_60,
# MAGIC     percentile_approx(ai_downtime, 0.7) AS ai_downtime_p_70,
# MAGIC     percentile_approx(ai_downtime, 0.8) AS ai_downtime_p_80,
# MAGIC     percentile_approx(ai_downtime, 0.90) AS ai_downtime_p_90,
# MAGIC     percentile_approx(ai_downtime, 0.95) AS ai_downtime_p_95,
# MAGIC     percentile_approx(ai_downtime, 0.96) AS ai_downtime_p_96,
# MAGIC     percentile_approx(ai_downtime, 0.97) AS ai_downtime_p_97,
# MAGIC     percentile_approx(ai_downtime, 0.98) AS ai_downtime_p_98,
# MAGIC     percentile_approx(ai_downtime, 0.99) AS ai_downtime_p_99
# MAGIC   FROM
# MAGIC     ai_downtime_table
# MAGIC   GROUP BY
# MAGIC     date
# MAGIC   ORDER BY
# MAGIC     date DESC
# MAGIC );

# COMMAND ----------

ai_downtime = spark.sql("select * from ai_downtime_metrics").toPandas()

# COMMAND ----------

# DBTITLE 1,TEST NEGATIVES
import random
from datetime import datetime, timedelta

TN = 0
FP = 0
date_list = ai_downtime.date.to_list()
for i in range(25):
    current_date = datetime.strptime(
        random.choice(date_list[0 : (len(date_list) - 30)]), "%Y-%m-%d"
    )
    has_positive = False
    for i in range(3):
        # season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date=detect(ai_downtime,time_column='date',value_column='ai_downtime_avg',frequency='d',data_type='continuous',task={'season':'MSTL','trend':'BOCPD'},zoom_in_point=current_date.strftime("%Y-%m-%d"),figsize=[18,3], period=[7])
        (
            season,
            trend,
            resid,
            deseason,
            outlier_x,
            outlier_y,
            impute_data,
            change_x,
            change_y,
            detected_lag,
            data_with_date,
        ) = detect(
            ai_downtime,
            time_column="date",
            value_column="ai_downtime_avg",
            frequency="d",
            data_type="continuous",
            task={"trend": "BOCPD"},
            zoom_in_point=current_date.strftime("%Y-%m-%d"),
            figsize=[18, 3],
            zoom_in_after=2,
        )
        if current_date in data_with_date[change_x]:
            has_positive = True
        current_date += timedelta(days=1)

    if has_positive == True:
        FP += 1
    else:
        TN += 1

print(TN)
print(FP)

# COMMAND ----------

# DBTITLE 1,TEST NEGATIVES + REMOVE YEARLY SEASONALITY
import random
from datetime import datetime, timedelta

TN = 0
FP = 0
date_list = ai_downtime.date.to_list()
for i in range(25):
    current_date = datetime.strptime(
        random.choice(date_list[0 : (len(date_list) - 730)]), "%Y-%m-%d"
    )
    has_positive = False
    for i in range(3):
        # season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date=detect(ai_downtime,time_column='date',value_column='ai_downtime_avg',frequency='d',data_type='continuous',task={'season':'MSTL','trend':'BOCPD'},zoom_in_point=current_date.strftime("%Y-%m-%d"),figsize=[18,3], period=[7])]]
        filtered_data = ai_downtime[
            ai_downtime["date"] <= current_date.strftime("%Y-%m-%d")
        ].reset_index(drop=True)

        (
            season,
            trend,
            resid,
            deseason,
            outlier_x,
            outlier_y,
            impute_data,
            change_x,
            change_y,
            detected_lag,
            data_with_date,
            _,
        ) = detect(
            filtered_data,
            time_column="date",
            value_column="ai_downtime_avg",
            frequency="d",
            data_type="continuous",
            task={"season": "MSTL"},
            periods=[7, 365],
        )

        trend_df = pd.DataFrame(
            {"date": data_with_date.index.strftime("%Y-%m-%d"), "value": trend}
        )

        (
            season,
            trend,
            resid,
            deseason,
            outlier_x,
            outlier_y,
            impute_data,
            change_x,
            change_y,
            detected_lag,
            data_with_date,
        ) = detect(
            trend_df,
            time_column="date",
            value_column="value",
            frequency="d",
            data_type="continuous",
            task={"trend": "BOCPD"},
            zoom_in_point=current_date.strftime("%Y-%m-%d"),
            figsize=[18, 3],
            zoom_in_after=2,
        )

        if current_date in data_with_date[change_x]:
            has_positive = True
        current_date += timedelta(days=1)

    if has_positive == True:
        FP += 1
    else:
        TN += 1

print(TN)
print(FP)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ai_downtime_metrics

# COMMAND ----------

# DBTITLE 1,TEST POSITIVES
from random import randint
from datetime import datetime

ai_downtime = spark.sql("select * from ai_downtime_metrics").toPandas()

devices_malfunction = 0.1
malfunction_by = 0.1

# Work backwards date, 25 times; just need to change data backwards
date_list = ai_downtime["date"].to_list()
date_index = 0

TP = 0
FN = 0

runs = 25
days = 0

for i in range(runs):
    print(date_list[date_index])

    has_positive = False
    ai_downtime.at[date_index, "ai_downtime_avg"] += (
        devices_malfunction * malfunction_by * 100
    )
    ai_downtime.at[date_index + 1, "ai_downtime_avg"] += (
        devices_malfunction * malfunction_by * 100
    )
    ai_downtime.at[date_index + 2, "ai_downtime_avg"] += (
        devices_malfunction * malfunction_by * 100
    )
    for j in range(3):
        current_date = datetime.strptime(date_list[date_index], "%Y-%m-%d")
        # season,trend,resid,deseason,outlier_x,outlier_y,impute_data,change_x,change_y,detected_lag,data_with_date=detect(ai_downtime,time_column='date',value_column='ai_downtime_avg',frequency='d',data_type='continuous',task={'season': 'MSTL', 'trend':'BOCPD'},zoom_in_point=current_date.strftime("%Y-%m-%d"),figsize=[18,3], period=[7])
        (
            season,
            trend,
            resid,
            deseason,
            outlier_x,
            outlier_y,
            impute_data,
            change_x,
            change_y,
            detected_lag,
            data_with_date,
        ) = detect(
            ai_downtime,
            time_column="date",
            value_column="ai_downtime_avg",
            frequency="d",
            data_type="continuous",
            task={"trend": "BOCPD"},
            zoom_in_point=current_date.strftime("%Y-%m-%d"),
            figsize=[18, 3],
            zoom_in_before=2,
        )
        if current_date in data_with_date[change_x]:
            has_positive = True
            days += 3 - j
        date_index += 1

    date_index += randint(0, 5)

    if has_positive:
        TP += 1
    else:
        FN += 1

print("TP (was detected): " + str(TP))
print("FN (was not detected): " + str(FN))
print("AVG TIME TO DETECTION: " + str(days / runs))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, recall_score, precision_score

# Create confusion matrix
conf_matrix = np.array([[TN, FP], [FN, TP]])

# Calculate recall and precision
recall = TP / (TP + FN)
precision = TP / (TP + FP)

# Print the results
print(f"Confusion Matrix:\n{conf_matrix}")
print(f"Recall: {recall:.2f}")
print(f"Precision: {precision:.2f}")
print("Of " + str(TP) + "/25 detected, average time to detection: " + str(days / runs))

# Plot confusion matrix
df_cm = pd.DataFrame(
    conf_matrix,
    index=["Actual Negative", "Actual Positive"],
    columns=["Predicted Negative", "Predicted Positive"],
)
plt.figure(figsize=(8, 6))
sns.heatmap(df_cm, annot=True, fmt="d", cmap="Blues")
plt.ylabel("Actual")
plt.xlabel("Predicted")
plt.title("Confusion Matrix")
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Testing
# MAGIC Requires to change each & every variable manually (i.e. devices malfunctioned, date, malfunction_by, what metric was analyzed, etc); used for validation on testing positives as the previous method is not possible to do with percentiles

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recording Uptime

# COMMAND ----------

# DBTITLE 1,recording_uptime
# MAGIC %sql
# MAGIC -- Define variables view
# MAGIC CREATE OR REPLACE TEMP VIEW variables AS (
# MAGIC   SELECT 0.05 AS devices_malfunction, '2021-06-01' AS firmware_rollout, -0.05 as malfunction_by
# MAGIC );
# MAGIC
# MAGIC -- Create device_details view
# MAGIC CREATE OR REPLACE TEMP VIEW device_details AS (
# MAGIC   SELECT DISTINCT
# MAGIC       device_id,
# MAGIC       CASE
# MAGIC         WHEN rand_value <= 0.012 THEN (SELECT firmware_rollout FROM variables)
# MAGIC         WHEN rand_value <= 0.028 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 3)  -- Add 3 days
# MAGIC         WHEN rand_value <= 0.068 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 6)  -- Add 6 days
# MAGIC         WHEN rand_value <= 0.158 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 9)  -- Add 9 days
# MAGIC         WHEN rand_value <= 0.338 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 12) -- Add 12 days
# MAGIC         ELSE DATE_ADD((SELECT firmware_rollout FROM variables), 15)  -- Add 15 days
# MAGIC       END AS stage,
# MAGIC       CASE
# MAGIC         WHEN rand_value2 <= (SELECT devices_malfunction FROM variables) THEN true
# MAGIC         ELSE false
# MAGIC       END AS abnormal
# MAGIC   FROM (
# MAGIC     -- Subquery to generate random values and filter by date
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         rand() AS rand_value,
# MAGIC         rand() AS rand_value2
# MAGIC     FROM hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC     WHERE date < DATE_ADD((SELECT firmware_rollout FROM variables), 15)  -- Add 15 days
# MAGIC   ) AS subquery
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Create heat_data view
# MAGIC CREATE OR REPLACE TEMP VIEW ru1 AS (
# MAGIC   SELECT
# MAGIC     d.date,
# MAGIC     d.device_id,
# MAGIC     SUM(d.total_trip_grace_recording_duration_ms) AS total_trip_grace_recording_duration_ms,
# MAGIC     SUM(d.total_trip_duration_ms) AS total_trip_duration_ms,
# MAGIC     a.stage,
# MAGIC     a.abnormal
# MAGIC   FROM hive_metastore.dataprep_safety.cm_device_health_daily d
# MAGIC   LEFT JOIN device_details a ON d.device_id = a.device_id
# MAGIC   WHERE d.date < DATE_ADD((SELECT firmware_rollout FROM variables), 30)
# MAGIC     AND d.date > DATE_ADD((SELECT firmware_rollout FROM variables), -100)
# MAGIC     AND a.stage = DATE_ADD((SELECT firmware_rollout FROM variables), 3) -- SELECT ONLY STAGE 1
# MAGIC   GROUP BY d.date, d.device_id, a.stage, a.abnormal
# MAGIC   ORDER BY d.date DESC, d.device_id
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW ru2 AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     CASE
# MAGIC       WHEN (stage <= date AND abnormal) THEN ((total_trip_grace_recording_duration_ms) / total_trip_duration_ms + (select malfunction_by from variables)) * 100
# MAGIC       ELSE (100 * (total_trip_grace_recording_duration_ms) / total_trip_duration_ms)
# MAGIC     END AS ru
# MAGIC   FROM ru1
# MAGIC );
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW recording_uptime_metrics AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     percentile_approx(ru, 0.5) AS median,
# MAGIC     avg(ru) AS avg,
# MAGIC     percentile_approx(ru, 0.1) AS p_10,
# MAGIC     percentile_approx(ru, 0.2) AS p_20,
# MAGIC     percentile_approx(ru, 0.3) AS p_30,
# MAGIC     percentile_approx(ru, 0.4) AS p_40,
# MAGIC     percentile_approx(ru, 0.5) AS p_50,
# MAGIC     percentile_approx(ru, 0.6) AS p_60,
# MAGIC     percentile_approx(ru, 0.7) AS p_70,
# MAGIC     percentile_approx(ru, 0.8) AS p_80,
# MAGIC     percentile_approx(ru, 0.9) AS p_90,
# MAGIC     percentile_approx(ru, 0.95) AS p_95,
# MAGIC     percentile_approx(ru, 0.96) AS p_96,
# MAGIC     percentile_approx(ru, 0.97) AS p_97,
# MAGIC     percentile_approx(ru, 0.98) AS p_98,
# MAGIC     percentile_approx(ru, 0.99) AS p_99
# MAGIC   FROM ru2
# MAGIC   GROUP BY date
# MAGIC   ORDER BY date DESC
# MAGIC );

# COMMAND ----------

# DBTITLE 1,recording_uptime
recording_uptime = spark.sql("select * from recording_uptime_metrics").toPandas()

# COMMAND ----------

# DBTITLE 1,recording_uptime
(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
) = detect(
    recording_uptime,
    time_column="date",
    value_column="p_20",
    frequency="d",
    data_type="continuous",
    task={"trend": "BOCPD"},
    zoom_in_point="06/01/2021",
    figsize=[18, 3],
)  # significance_level=0.05


# COMMAND ----------

# MAGIC %md
# MAGIC #### AI Downtime

# COMMAND ----------

# DBTITLE 1,ONLY STAGE 1 SELECTED (ai_downtime)
# MAGIC %sql
# MAGIC -- Define variables view
# MAGIC CREATE OR REPLACE TEMP VIEW variables AS (
# MAGIC   SELECT 0.0 AS devices_malfunction, '2021-05-01' AS firmware_rollout, 0.0 as malfunction_by
# MAGIC );
# MAGIC
# MAGIC -- Create device_details view
# MAGIC CREATE OR REPLACE TEMP VIEW device_details AS (
# MAGIC   SELECT DISTINCT
# MAGIC       device_id,
# MAGIC       CASE
# MAGIC         WHEN rand_value <= 0.012 THEN (SELECT firmware_rollout FROM variables)
# MAGIC         WHEN rand_value <= 0.028 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 3)  -- Add 3 days
# MAGIC         WHEN rand_value <= 0.068 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 6)  -- Add 6 days
# MAGIC         WHEN rand_value <= 0.158 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 9)  -- Add 9 days
# MAGIC         WHEN rand_value <= 0.338 THEN DATE_ADD((SELECT firmware_rollout FROM variables), 12) -- Add 12 days
# MAGIC         ELSE DATE_ADD((SELECT firmware_rollout FROM variables), 15)  -- Add 15 days
# MAGIC       END AS stage,
# MAGIC       CASE
# MAGIC         WHEN rand_value2 <= (SELECT devices_malfunction FROM variables) THEN true
# MAGIC         ELSE false
# MAGIC       END AS abnormal
# MAGIC   FROM (
# MAGIC     -- Subquery to generate random values and filter by date
# MAGIC     SELECT
# MAGIC         device_id,
# MAGIC         rand() AS rand_value,
# MAGIC         rand() AS rand_value2
# MAGIC     FROM hive_metastore.dataprep_safety.cm_device_health_daily
# MAGIC     WHERE date < DATE_ADD((SELECT firmware_rollout FROM variables), 15)  -- Add 15 days
# MAGIC   ) AS subquery
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Create heat_data view
# MAGIC CREATE OR REPLACE TEMP VIEW heat_data AS (
# MAGIC   SELECT
# MAGIC     d.date,
# MAGIC     d.device_id,
# MAGIC     SUM(d.total_safe_mode_duration_ms) AS total_safe_mode_duration_ms,
# MAGIC     SUM(d.total_overheated_duration_ms) AS total_overheated_duration_ms,
# MAGIC     SUM(d.total_trip_duration_ms) AS total_trip_duration_ms,
# MAGIC     a.stage,
# MAGIC     a.abnormal
# MAGIC   FROM hive_metastore.dataprep_safety.cm_device_health_daily d
# MAGIC   LEFT JOIN device_details a ON d.device_id = a.device_id
# MAGIC   WHERE d.date < DATE_ADD((SELECT firmware_rollout FROM variables), 30)
# MAGIC     AND d.date > DATE_ADD((SELECT firmware_rollout FROM variables), -100)
# MAGIC     AND a.stage = DATE_ADD((SELECT firmware_rollout FROM variables), 3) -- SELECT ONLY STAGE 1
# MAGIC   GROUP BY d.date, d.device_id, a.stage, a.abnormal
# MAGIC   ORDER BY d.date DESC, d.device_id
# MAGIC );
# MAGIC
# MAGIC -- Create ai_downtime_table view
# MAGIC CREATE OR REPLACE TEMP VIEW ai_downtime_table AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     CASE
# MAGIC       WHEN (stage <= date AND abnormal) THEN ((total_safe_mode_duration_ms + total_overheated_duration_ms) / total_trip_duration_ms + (select malfunction_by from variables)) * 100
# MAGIC       ELSE (100 * (total_safe_mode_duration_ms + total_overheated_duration_ms) / total_trip_duration_ms)
# MAGIC     END AS ai_downtime
# MAGIC   FROM heat_data
# MAGIC );
# MAGIC
# MAGIC -- Create ai_downtime_metrics view
# MAGIC CREATE OR REPLACE TEMP VIEW ai_downtime_metrics AS (
# MAGIC   SELECT
# MAGIC     date,
# MAGIC     percentile_approx(ai_downtime, 0.5) AS ai_downtime_median,
# MAGIC     avg(ai_downtime) AS ai_downtime_avg,
# MAGIC     percentile_approx(ai_downtime, 0.1) AS ai_downtime_p_10,
# MAGIC     percentile_approx(ai_downtime, 0.2) AS ai_downtime_p_20,
# MAGIC     percentile_approx(ai_downtime, 0.3) AS ai_downtime_p_30,
# MAGIC     percentile_approx(ai_downtime, 0.4) AS ai_downtime_p_40,
# MAGIC     percentile_approx(ai_downtime, 0.5) AS ai_downtime_p_50,
# MAGIC     percentile_approx(ai_downtime, 0.6) AS ai_downtime_p_60,
# MAGIC     percentile_approx(ai_downtime, 0.7) AS ai_downtime_p_70,
# MAGIC     percentile_approx(ai_downtime, 0.8) AS ai_downtime_p_80,
# MAGIC     percentile_approx(ai_downtime, 0.9) AS ai_downtime_p_90,
# MAGIC     percentile_approx(ai_downtime, 0.95) AS ai_downtime_p_95,
# MAGIC     percentile_approx(ai_downtime, 0.96) AS ai_downtime_p_96,
# MAGIC     percentile_approx(ai_downtime, 0.97) AS ai_downtime_p_97,
# MAGIC     percentile_approx(ai_downtime, 0.98) AS ai_downtime_p_98,
# MAGIC     percentile_approx(ai_downtime, 0.99) AS ai_downtime_p_99
# MAGIC   FROM ai_downtime_table
# MAGIC   GROUP BY date
# MAGIC   ORDER BY date DESC
# MAGIC );
# MAGIC

# COMMAND ----------

# DBTITLE 1,ai_downtime
ai_downtime = spark.sql("select * from ai_downtime_metrics").toPandas()

# COMMAND ----------

# DBTITLE 1,ai_downtime
# Change the value column as necessary (to see which percentiles detect which anomalies [for figuring out which percentiles is optimal])
(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_avg",
    frequency="d",
    data_type="continuous",
    task={"trend": "BOCPD"},
    zoom_in_point="05/01/2021",
    figsize=[18, 3],
    zoom_in_after=2,
)

# COMMAND ----------

# DBTITLE 1,ai_downtime
(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
    _,
) = detect(
    ai_downtime,
    time_column="date",
    value_column="ai_downtime_p_95",
    frequency="d",
    data_type="continuous",
    task={"season": "MSTL", "trend": "BOCPD"},
    zoom_in_point="04/06/2021",
    figsize=[18, 3],
    periods=[7, 365],
    zoom_in_after=2,
)

# COMMAND ----------

from datetime import datetime, timedelta

end_date = datetime.strptime(ai_downtime.date[0], "%Y-%m-%d")
current_date = datetime.strptime(ai_downtime.date[30 + 30], "%Y-%m-%d")

anomalous_dates = []

while current_date <= end_date:
    (
        season,
        trend,
        resid,
        deseason,
        outlier_x,
        outlier_y,
        impute_data,
        change_x,
        change_y,
        detected_lag,
        data_with_date,
        _,
    ) = detect(
        ai_downtime,
        time_column="date",
        value_column="ai_downtime_avg",
        frequency="d",
        data_type="continuous",
        task={"season": "MSTL", "trend": "BOCPD"},
        zoom_in_point=current_date.strftime("%Y-%m-%d"),
        figsize=[18, 3],
        periods=[7],
        zoom_in_after=2,
    )
    if current_date in data_with_date[change_x]:
        anomalous_dates.append(current_date)
        print("added: ")
    print(current_date)
    current_date += timedelta(days=1)

print(anomalous_dates)

# COMMAND ----------

from datetime import datetime, timedelta

end_date = datetime.strptime(ai_downtime.date[0], "%Y-%m-%d")
current_date = datetime.strptime(ai_downtime.date[30 + 30], "%Y-%m-%d")

anomalous_dates = []

while current_date <= end_date:
    (
        season,
        trend,
        resid,
        deseason,
        outlier_x,
        outlier_y,
        impute_data,
        change_x,
        change_y,
        detected_lag,
        data_with_date,
    ) = detect(
        ai_downtime,
        time_column="date",
        value_column="ai_downtime_avg",
        frequency="d",
        data_type="continuous",
        task={"trend": "BOCPD"},
        zoom_in_point=current_date.strftime("%Y-%m-%d"),
        figsize=[18, 3],
        zoom_in_after=2,
    )
    if current_date in data_with_date[change_x]:
        anomalous_dates.append(current_date)
        print("added: ")
    print(current_date)
    current_date += timedelta(days=1)

print(anomalous_dates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Random / One off testing

# COMMAND ----------

(
    season,
    trend,
    resid,
    deseason,
    outlier_x,
    outlier_y,
    impute_data,
    change_x,
    change_y,
    detected_lag,
    data_with_date,
    _,
) = detect(
    hwatchdog,
    time_column="date",
    value_column="hwatchdog_avg",
    frequency="d",
    data_type="continuous",
    task={"season": "MSTL", "trend": "Dynp", "outlier": "spline_isoForest"},
    periods=[7, 28, 365],
)

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np

y = np.array(data_with_date.to_list())
y = y.reshape(
    -1,
)
x = data_with_date.index

fig, ax = plt.subplots(figsize=[18, 3])
ax.plot(x, y)
ax.scatter(
    data_with_date.index[outlier_x], y[outlier_x], color="red", label="outlier points"
)
ax.scatter(
    data_with_date.index[change_x],
    y[change_x],
    color="blue",
    label="trend segmentation point",
)
ax.set_title("Original Data and Change Points")

x_min, x_max = x.min(), x.max()

for firmware_rollout in firmware_rollouts.date:
    if x_min <= firmware_rollout <= x_max:
        ax.axvline(x=firmware_rollout, color="black", linestyle="--")

ax.legend()
