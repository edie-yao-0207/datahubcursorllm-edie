# Databricks notebook source
# MAGIC %run /backend/dataproducts/cityinsights/city_insights_datasets

# COMMAND ----------

# MAGIC %run backend/backend/databricks_data_alerts/alerting_system

# COMMAND ----------

spark.conf.set("spark.databricks.queryWatchdog.maxQueryTasks", 200000)

# COMMAND ----------

from datetime import datetime, timedelta

import pandas as pd

today = datetime.today().strftime("%Y-%m-%d")
lastYearToday = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")

generate_harsh_event_density_dataset("boston", "ma", "Eastern", lastYearToday, today)
generate_speed_dataset("boston", "ma", "Eastern", lastYearToday, today)

generate_harsh_event_density_dataset("houston", "tx", "Central", lastYearToday, today)
generate_speed_dataset("houston", "tx", "Central", lastYearToday, today)

generate_harsh_event_density_dataset("chicago", "il", "Central", lastYearToday, today)
generate_speed_dataset("chicago", "il", "Central", lastYearToday, today)

generate_harsh_event_density_dataset("new_york", "ny", "Eastern", lastYearToday, today)
generate_speed_dataset("new_york", "ny", "Eastern", lastYearToday, today)

generate_harsh_event_density_dataset(
    "los_angeles", "ca", "Pacific", lastYearToday, today
)
generate_speed_dataset("los_angeles", "ca", "Pacific", lastYearToday, today)

generate_harsh_event_density_dataset("atlanta", "ga", "Eastern", lastYearToday, today)
generate_speed_dataset("atlanta", "ga", "Eastern", lastYearToday, today)


# COMMAND ----------


cities = ["boston", "houston", "chicago", "atlanta", "los_angeles", "new_york"]
metric_types = ["nhe", "speed"]
map_types = ["rs", "n"]
file_name_to_row_count_dict = {}

successfulDatasets = ""
unsuccessfulDatasets = ""
for city in cities:
    for metric_type in metric_types:
        for map_type in map_types:
            name = "{}_{}_{}".format(city, metric_type, map_type)
            table = "playground.{}".format(name)

            query = "describe detail {}".format(table)

            lastModified = spark.sql(query).toPandas()[["lastModified"]].to_dict()
            lastModifiedDatetime = datetime.fromisoformat(
                str(lastModified["lastModified"][0])
            )

            today = datetime.today()
            if lastModifiedDatetime > today - timedelta(days=10):
                successfulDatasets = successfulDatasets + ", " + name
            else:
                unsuccessfulDatasets = unsuccessfulDatasets + ", " + name

            file_row_count_query = "select count(1) from {}".format(table)
            file_name_to_row_count_dict[name] = (
                spark.sql(file_row_count_query)
                .toPandas()[["count(1)"]]
                .to_dict()["count(1)"][0]
            )


slack_channels = ["alerts-data-science"]
alert_name = "dataproducts_monthly_city_insights_data_refresh_alert"
emails = ["joanne.wang@samsara.com"]
custom_message = """
Monthly City Insights Data Refresh complete.
Successful datasets: {}
Unsuccessful datasets: {}
""".format(
    successfulDatasets, unsuccessfulDatasets
)

df = pd.DataFrame.from_dict(file_name_to_row_count_dict, orient="index").reset_index()
df.columns = ["file_name", "row_count"]
data_row_counts_sdf = spark.createDataFrame(df)

_execute_alert(data_row_counts_sdf, emails, slack_channels, alert_name, custom_message)


# COMMAND ----------

cities = ["boston", "houston", "chicago", "atlanta", "los_angeles", "new_york"]
metric_types = ["nhe", "speed"]
map_types = ["rs", "n"]

for city in cities:
    for metric_type in metric_types:
        for map_type in map_types:
            name = "{}_{}_{}".format(city, metric_type, map_type)
            table = "playground.{}".format(name)
            sdf = spark.table(table).select("*")
            sdf.coalesce(1).write.format("csv").mode("overwrite").option(
                "header", True
            ).save("s3://samsara-data-insights/temp/{}".format(name))
            # since filename is stored with a random hash, we need
            # to find it and move to a different path and rename
            # so kepler is able to fetch via a static filepath
            writePath = "s3://samsara-data-insights/temp/{}".format(name)
            readPath = "s3://samsara-data-insights/temp/{}".format(name)
            file_list = dbutils.fs.ls(readPath)
            for i in file_list:
                if i[1].startswith("part-"):
                    read_name = i[1]
            dbutils.fs.cp(
                readPath + "/" + read_name, writePath + "/" + "{}.csv".format(name)
            )
            dbutils.fs.rm(readPath + "/" + read_name)
