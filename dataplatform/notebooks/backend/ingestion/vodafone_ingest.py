# Databricks notebook source
# MAGIC %sql
# MAGIC -- Use dynamic partitioning when writing to trusted tables so that we avoid ingesting all past data
# MAGIC -- in S3 given to us by Vodafone.
# MAGIC SET spark.sql.sources.partitionOverwriteMode=DYNAMIC;
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled = True;

# COMMAND ----------

from datetime import date, datetime, timedelta
from typing import Dict, List

from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    DataType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException


def spec_to_schema(spec: Dict[str, str]) -> StructType:
    fields: List[StructField] = []
    for colname, coltype in spec:
        fieldname = (
            colname.translate(str.maketrans(" -()", "____"))
            .replace("___", "_")
            .replace("__", "_")
            .lower()
        )
        fieldtype: DataType = StringType()
        if coltype == "Integer":
            fieldtype = LongType()
        elif coltype == "Date":
            fieldtype = DateType()
        fields.append(StructField(fieldname, fieldtype))
    return StructType(fields)


"""Load the Vodafone data with schema."""

data_usage_spec = [
    ("Day", "Date"),
    ("ICCID", "AlphaNumeric"),
    ("Sum Bytes upload", "Integer"),
    ("Sum Bytes download", "Integer"),
    ("Sum Total Kbytes", "Float"),
    ("Sum Bytes upload Test", "Integer"),
    ("Sum Bytes download Test", "Integer"),
    ("Sum Total KBytes Test", "Float"),
    ("Nr. of PS Sessions", "Integer"),
]

today = datetime.utcnow().date()
yesterday = today - timedelta(days=1)
yesterdayYearMonth = yesterday.strftime("%Y%m")
yesterdayYearMonthRegex = "{0}*".format(yesterdayYearMonth)

priorMonth = yesterday + relativedelta(months=-1)
priorMonthYearMonth = priorMonth.strftime("%Y%m")
priorMonthYearMonthRegex = "{0}*".format(priorMonthYearMonth)

# We want to grab all the data in the past month and write it to our table. Our regex looks for data start from yesterday since
# the filename always starts with yesterday's date. Then we get the prior month from that day and define our path respectively.
path1 = "s3://samsara-vodafone-reports/Summarised_data_usage_per_ICCID_(STCU)_{0}.[Cc][Ss][Vv]".format(
    yesterdayYearMonthRegex
)
path2 = "s3://samsara-vodafone-reports/Summarised_data_usage_per_ICCID_(STCU)_{0}.[Cc][Ss][Vv]".format(
    priorMonthYearMonthRegex
)
paths = "{0},{1}".format(path1, path2)

print(
    "Loading data from files matching the following dates: {0}, {1}".format(
        priorMonthYearMonth, yesterdayYearMonth
    )
)


spark = SparkSession.builder.getOrCreate()
reader_options = {
    "header": "true",
    "sep": ",",
    "timestampFormat": "yyyy-MM-dd",
    "mode": "PERMISSIVE",
}

reader = spark.read.format("csv").options(**reader_options)

# We attempt to process the data from the current + past month from S3. If the current month's data is not present it will fail. (can happen on first or second day of month)
# We explicity catch this error and only try to process the previous month's data if this is the case.
try:
    vodafone_csv_data = (
        reader.schema(spec_to_schema(data_usage_spec))
        .load(paths.split(",")[0])
        .withColumn("filename", F.input_file_name())
    )
    vodafone_csv_data.createOrReplaceTempView("vodafone_csv_data")
except AnalysisException as x:
    print(
        str(x)
        + "\nCould not load current month path. Continuing with previous month {0}".format(
            priorMonthYearMonth
        )
    )
    vodafone_csv_data = (
        reader.schema(spec_to_schema(data_usage_spec))
        .load(paths.split(",")[1])
        .withColumn("filename", F.input_file_name())
    )
    vodafone_csv_data.createOrReplaceTempView("vodafone_csv_data")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or replace temp view vodafone_daily_usage as (
# MAGIC   SELECT
# MAGIC     DATE(day),
# MAGIC     iccid,
# MAGIC     SUM(sum_total_kbytes) as data_usage
# MAGIC   FROM vodafone_csv_data
# MAGIC   GROUP BY
# MAGIC     DATE(day),
# MAGIC     iccid
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dataprep_cellular.vodafone_daily_usage
# MAGIC USING delta PARTITIONED BY (day)
# MAGIC AS
# MAGIC SELECT * FROM vodafone_daily_usage LIMIT 0;
# MAGIC
# MAGIC ALTER TABLE dataprep_cellular.vodafone_daily_usage CHANGE data_usage COMMENT 'unit is kilobytes';

# COMMAND ----------

# Use pyspark SQL to do delta overwrites to avoid having to rewrite
# the entire table each time this notebook is ran. We go 28 days
# in the past so that we are sure we only overwrite data that is for
# sure in the past two months since we filter source data only
# from the past two months. Since the shortest month (February) can
# have at most 28 days, we want to avoid the situation where we
# are at the start of March, and we go further than February first,
# (i.e., we overwrite data in January that we haven't fetched from
# S3).

spark.table("vodafone_daily_usage").filter(
    "day >= date_sub(current_date(),28)"
).write.format("delta").mode("overwrite").partitionBy("day").option(
    "replaceWhere", "day >= date_sub(current_date(),28)"
).option(
    "mergeSchema", "true"
).saveAsTable(
    "dataprep_cellular.vodafone_daily_usage"
)
