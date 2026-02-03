from pyspark.sql.functions import to_date, col

spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_device_info"
).load().withColumnRenamed("Date", "timestamp").withColumn(
    "date", to_date(col("timestamp"))
).write.format(
    "delta"
).mode(
    "ignore"
).partitionBy(
    "date"
).saveAsTable(
    "mobile_logs.mobile_troy_device_info_ingestion"
)

spark.read.format("bigquery").option(
    "table", "backend.mobile_troy_device_info"
).load().withColumnRenamed("Date", "timestamp").withColumn(
    "date", to_date(col("timestamp"))
).filter(
    "date >= date_sub(current_date(),14)"
).createOrReplaceTempView(
    "mobile_troy_device_info_ingestion_updates"
)
spark.table("mobile_troy_device_info_ingestion_updates").write.format("delta").mode(
    "overwrite"
).partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(),14)"
).saveAsTable(
    "mobile_logs.mobile_troy_device_info_ingestion"
)

spark.table(
    "mobile_logs.mobile_troy_device_info_ingestion"
).dropDuplicates().createOrReplaceTempView("mobile_troy_device_info_history")

spark.table("mobile_troy_device_info_history").write.format("delta").partitionBy(
    "date"
).mode("ignore").saveAsTable("mobile_logs.mobile_troy_device_info")

mobile_logs = spark.sql(
    """
                        select * 
                        from mobile_logs.mobile_troy_device_info_ingestion 
                        where date >= date_sub(current_date(),14)
                        """
)

mobile_logs = mobile_logs.dropDuplicates()

mobile_logs.write.format("delta").mode("overwrite").partitionBy("date").option(
    "replaceWhere", "date >= date_sub(current_date(),14)"
).saveAsTable("mobile_logs.mobile_troy_device_info")
