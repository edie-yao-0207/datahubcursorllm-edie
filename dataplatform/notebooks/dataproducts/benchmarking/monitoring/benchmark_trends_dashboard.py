# Databricks notebook source
# MAGIC %md
# MAGIC # Widgets

# COMMAND ----------

dbutils.widgets.remove("Metric")
dbutils.widgets.remove("Type of Benchmark")

dbutils.widgets.multiselect(
    "Metric",
    "Harsh Accel",
    [
        "Harsh Accel",
        "Harsh Brake",
        "Harsh Turn",
        "Harsh Events All",
        "Light Speeding",
        "Moderate Speeding",
        "Heavy Speeding",
        "Severe Speeding",
        "All Speeding",
        "Idling Percent",
    ],
)

dbutils.widgets.multiselect("Type of Benchmark", "Mean", ["Mean", "Median", "Top 10%"])

# dbutils.widgets.dropdown('Metric', 'Harsh Accel', ['Harsh Accel', 'Harsh Brake', 'Harsh Turn', 'Harsh Events All', 'Light Speeding', 'Moderate Speeding', 'Heavy Speeding', 'Severe Speeding'])
# dbutils.widgets.dropdown("Type of Benchmark", "Mean", ['Mean', 'Median', 'Top 10%'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Definitions & Helpers

# COMMAND ----------

# Using Seaborn
from bokeh.plotting import figure, output_file, show
from bokeh.resources import CDN
from bokeh.embed import components, file_html
import seaborn as sns
import matplotlib.pyplot as plt

metric_table_name = {
    "Harsh Accel": "playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "Harsh Brake": "playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "Harsh Turn": "playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "Harsh Events All": "playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "Light Speeding": "dataproducts.light_speeding_benchmarks_by_week",
    "Moderate Speeding": "playground.moderate_speeding_benchmarks_by_week",
    "Heavy Speeding": "playground.heavy_speeding_benchmarks_by_week",
    "Severe Speeding": "playground.severe_speeding_benchmarks_by_week",
    "All Speeding": "playground.all_speeding_benchmarks_by_week",
    "Idling Percent": "playground.idling_percent_benchmarks_by_week",
}

benchmark_column_names = {
    "Mean": "mean",
    "Median": "median",
    "Top 10%": "top10_percentile",
}

metric_values = dbutils.widgets.get("Metric").split(",")
type_of_benchmark_values = dbutils.widgets.get("Type of Benchmark").split(",")


def get_max_benchmark_trends_value(metric_value, type_of_benchmark_value):
    table_name = metric_table_name.get(metric_value)
    benchmark_col = benchmark_column_names.get(type_of_benchmark_value)
    if (
        table_name == "playground.idling_percent_benchmarks_by_week"
        or "speeding" in table_name
    ):
        return 100
    query = "select * from {table_name}".format(table_name=table_name)
    pd_df = spark.sql(query).toPandas()
    return pd_df[benchmark_col].max() + 0.05


def display_benchmark_dashboard(metric_value, type_of_benchmark_value, y_max):
    table_name = metric_table_name.get(metric_value)
    benchmark_col = benchmark_column_names.get(type_of_benchmark_value)
    query = "select * from {table_name} order by week".format(table_name=table_name)
    pd_df = spark.sql(query).toPandas()
    order = ["00", "01", "02", "03", "10", "11", "12", "13", "20", "21", "22", "23"]
    # g = sns.FacetGrid(pd_df, col="cohort_id", col_wrap=4, height=2, ylim=(0, y_max))
    # g.map(sns.pointplot, "week", benchmark_col, color=".003", scale=.2)
    g = sns.catplot(
        data=pd_df,
        x="week",
        y=benchmark_col,
        col="cohort_id",
        col_wrap=4,
        height=2,
        scale=0.2,
        kind="point",
    )
    g.set(ylim=(0, y_max))
    for ax in g.axes.flat:
        labels = ax.get_xticklabels()
        for i, l in enumerate(labels):
            if i % 10 != 0:
                labels[i] = ""
        ax.set_xticklabels(labels, rotation=90)
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(metric_value + " -- " + type_of_benchmark_value)
    return g


def get_max_benchmark_value(
    table_osda, table_safetydb_1, table_safetydb_2, benchmark_type
):
    query_osda = "select * from {table}".format(table=table_osda)
    query_safetydb_1 = "select * from {table}".format(table=table_safetydb_1)
    query_safetydb_2 = "select * from {table}".format(table=table_safetydb_2)
    pd_df_osda = spark.sql(query_osda).toPandas()
    pd_df_safetydb_1 = spark.sql(query_safetydb_1).toPandas()
    pd_df_safetydb_2 = spark.sql(query_safetydb_2).toPandas()
    max_osda = pd_df_osda[benchmark_type].max() + 0.01
    max_safetydb_1 = pd_df_safetydb_1[benchmark_type].max() + 0.01
    max_safetydb_2 = pd_df_safetydb_2[benchmark_type].max() + 0.01
    if max_osda > max_safetydb_1:
        if max_osda > max_safetydb_2:
            return max_osda
        else:
            return max_safetydb_2
    else:
        if max_safetydb_1 > max_safetydb_2:
            return max_safetydb_1
        else:
            return max_safetydb_2


def display_plots(table_name, table_descr, benchmark_type, y_max):
    query = "select * from {table_name}".format(table_name=table_name)
    pd_df = spark.sql(query).toPandas()
    order = ["00", "01", "02", "03", "10", "11", "12", "13", "20", "21", "22", "23"]
    # g = sns.factorplot(data = pd_df, x = "week", y =benchmark_type, col="cohort_id", col_wrap=4, height=2, ylim=(0, y_max), scale=.2, dropna=False)
    g = sns.catplot(
        data=pd_df,
        x="week",
        y=benchmark_type,
        col="cohort_id",
        col_wrap=4,
        height=2,
        scale=0.2,
        kind="point",
    )
    g.set(ylim=(0, y_max))
    for ax in g.axes.flat:
        labels = ax.get_xticklabels()
        for i, l in enumerate(labels):
            if i % 10 != 0:
                labels[i] = ""
        ax.set_xticklabels(labels, rotation=90)  # set new labels
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(
        "harshaccel -- {table_descr} -- {benchmark_type}".format(
            table_descr=table_descr, benchmark_type=benchmark_type
        )
    )
    display(g.fig)


def display_plots_stacked(metric_type, table_name, table_descr, benchmark_type, y_max):
    query = "select * from {table_name}".format(table_name=table_name)
    pd_df = spark.sql(query).toPandas()
    order = ["00", "01", "02", "03", "10", "11", "12", "13", "20", "21", "22", "23"]
    my_hue_order = [
        "osDAccelerometer",
        "safetydb_including_orgs_with_custom_thresholds",
        "safetydb_excluding_orgs_with_custom_thresholds",
    ]
    # g = sns.factorplot(data = pd_df, x = "week", y = benchmark_type, col="cohort_id", col_wrap=4, height=2, ylim=(0, y_max), hue="metric", hue_order = my_hue_order, legend_out=True, scale=.2, dropna=False)
    g = sns.catplot(
        data=pd_df,
        x="week",
        y=benchmark_type,
        col="cohort_id",
        col_wrap=4,
        height=2,
        hue="metric",
        hue_order=my_hue_order,
        legend_out=True,
        scale=0.2,
        kind="point",
    )
    g.set(ylim=(0, y_max))
    for lh in g._legend.legendHandles:
        lh.set_alpha(1)
        lh._sizes = [50]
    for ax in g.axes.flat:
        labels = ax.get_xticklabels()
        for i, l in enumerate(labels):
            if i % 10 != 0:
                labels[i] = ""
        ax.set_xticklabels(labels, rotation=90)  # set new labels
    plt.subplots_adjust(top=0.9)
    g.fig.suptitle(
        "{metric_type} -- {table_descr} -- {benchmark_type}".format(
            metric_type=metric_type,
            table_descr=table_descr,
            benchmark_type=benchmark_type,
        )
    )
    display(g.fig)


# COMMAND ----------

# MAGIC %md
# MAGIC # Benchmark Metric Trends

# COMMAND ----------

for metric_value in metric_values:
    for type_of_benchmark_value in type_of_benchmark_values:
        y_max = get_max_benchmark_trends_value(metric_value, type_of_benchmark_value)
        graph = display_benchmark_dashboard(
            metric_value, type_of_benchmark_value, y_max
        )
        display(graph.fig)
        plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Comparing benchmarking plots for the following data :
# MAGIC ### osdaccelerometer vs.
# MAGIC ### safetydb excluding orgs with custom thresholds vs.
# MAGIC ### safetydb including orgs with custom thresholds

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view all_harsh_accel as
# MAGIC select * from
# MAGIC (
# MAGIC   select *, "osDAccelerometer" as metric
# MAGIC     from playground.harsh_accel_benchmarks_by_week_osda
# MAGIC   union
# MAGIC   select *, "safetydb_including_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   union
# MAGIC   select *, "safetydb_excluding_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC ) order by week, cohort_id;
# MAGIC
# MAGIC cache table all_harsh_accel;
# MAGIC
# MAGIC create or replace temp view all_harsh_brake as
# MAGIC select * from
# MAGIC (
# MAGIC   select *, "osDAccelerometer" as metric
# MAGIC     from playground.harsh_brake_benchmarks_by_week_osda
# MAGIC   union
# MAGIC   select *, "safetydb_including_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   union
# MAGIC   select *, "safetydb_excluding_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC ) order by week, cohort_id;
# MAGIC
# MAGIC cache table all_harsh_brake;
# MAGIC
# MAGIC create or replace temp view all_harsh_turn as
# MAGIC select * from
# MAGIC (
# MAGIC   select *, "osDAccelerometer" as metric
# MAGIC     from playground.harsh_turn_benchmarks_by_week_osda
# MAGIC   union
# MAGIC   select *, "safetydb_including_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   union
# MAGIC   select *, "safetydb_excluding_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC ) order by week, cohort_id;
# MAGIC
# MAGIC cache table all_harsh_turn;
# MAGIC
# MAGIC create or replace temp view all_harsh_events as
# MAGIC select * from
# MAGIC (
# MAGIC   select *, "osDAccelerometer" as metric
# MAGIC     from playground.harsh_events_all_benchmarks_by_week_osda
# MAGIC   union
# MAGIC   select *, "safetydb_including_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs
# MAGIC   union
# MAGIC   select *, "safetydb_excluding_orgs_with_custom_thresholds" as metric
# MAGIC     from playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs
# MAGIC ) order by week, cohort_id;
# MAGIC
# MAGIC cache table all_harsh_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Accel

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_accel_benchmarks_by_week_osda",
    "playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "mean",
)
display_plots_stacked(
    "harsh accel",
    "all_harsh_accel",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "mean",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_accel_benchmarks_by_week_osda",
    "playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "median",
)
display_plots_stacked(
    "harsh accel",
    "all_harsh_accel",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "median",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_accel_benchmarks_by_week_osda",
    "playground.harsh_accel_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_accel_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "top10_percentile",
)
display_plots_stacked(
    "harsh accel",
    "all_harsh_accel",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "top10_percentile",
    y_max,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Brake

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_brake_benchmarks_by_week_osda",
    "playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "mean",
)
display_plots_stacked(
    "harsh brake",
    "all_harsh_brake",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "mean",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_brake_benchmarks_by_week_osda",
    "playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "median",
)
display_plots_stacked(
    "harsh brake",
    "all_harsh_brake",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "median",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_brake_benchmarks_by_week_osda",
    "playground.harsh_brake_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_brake_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "top10_percentile",
)
display_plots_stacked(
    "harsh brake",
    "all_harsh_brake",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "top10_percentile",
    y_max,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Turn

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_turn_benchmarks_by_week_osda",
    "playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "mean",
)
display_plots_stacked(
    "harsh turn",
    "all_harsh_turn",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "mean",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_turn_benchmarks_by_week_osda",
    "playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "median",
)
display_plots_stacked(
    "harsh turn",
    "all_harsh_turn",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "median",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_turn_benchmarks_by_week_osda",
    "playground.harsh_turn_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_turn_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "top10_percentile",
)
display_plots_stacked(
    "harsh turn",
    "all_harsh_turn",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "top10_percentile",
    y_max,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Harsh Events All

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_events_all_benchmarks_by_week_osda",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "mean",
)
display_plots_stacked(
    "harsh events all",
    "all_harsh_events",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "mean",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_events_all_benchmarks_by_week_osda",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "median",
)
display_plots_stacked(
    "harsh events all",
    "all_harsh_events",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "median",
    y_max,
)

# COMMAND ----------

y_max = get_max_benchmark_value(
    "playground.harsh_events_all_benchmarks_by_week_osda",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_including_custom_threshold_orgs",
    "playground.harsh_events_all_benchmarks_by_week_safetydb_excluding_custom_threshold_orgs",
    "top10_percentile",
)
display_plots_stacked(
    "harsh events all",
    "all_harsh_events",
    "osda and safetydb all and safetydb excluding orgs with custom thresholds",
    "top10_percentile",
    y_max,
)

# COMMAND ----------
