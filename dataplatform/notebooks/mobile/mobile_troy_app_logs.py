from pyspark.sql.functions import to_date, col

spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_app_logs"
).load().withColumnRenamed("Date", "timestamp").withColumn(
    "date", to_date(col("timestamp"))
).write.format(
    "delta"
).mode(
    "ignore"
).partitionBy(
    "date"
).saveAsTable(
    "mobile_logs.mobile_app_log_events_ingestion"
)

spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_app_logs"
).load().withColumnRenamed("Date", "timestamp").withColumn(
    "date", to_date(col("timestamp"))
).filter(
    "date >= date_sub(current_date(),14)"
).createOrReplaceTempView(
    "mobile_app_log_events_ingestion_updates"
)
spark.table("mobile_app_log_events_ingestion_updates").write.format("delta").mode(
    "overwrite"
).partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(),14)"
).saveAsTable(
    "mobile_logs.mobile_app_log_events_ingestion"
)

spark.table(
    "mobile_logs.mobile_app_log_events_ingestion"
).dropDuplicates().createOrReplaceTempView("mobile_app_logs_history")

spark.table("mobile_app_logs_history").write.format("delta").partitionBy("date").mode(
    "ignore"
).saveAsTable("mobile_logs.mobile_app_log_events")

mobile_logs = spark.sql(
    """
                        select * 
                        from mobile_logs.mobile_app_log_events_ingestion
                        where date >= date_sub(current_date(),14)
                        """
)

mobile_logs = mobile_logs.dropDuplicates()

mobile_logs.write.format("delta").mode("overwrite").partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(),14)"
).saveAsTable("mobile_logs.mobile_app_log_events")
