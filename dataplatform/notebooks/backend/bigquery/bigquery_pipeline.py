from pyspark.sql.functions import to_date, col


def bigquery_pipeline(bq_table, date_column, dest_db, dest_table, days="2"):

    ingestion_table = f"{dest_db}.{dest_table}_ingestion"
    ingestion_view = f"{dest_table}_ingestion_updates"
    history_view = f"{dest_table}_history"
    table = f"{dest_db}.{dest_table}"

    spark.read.format("bigquery").option("table", bq_table).load().withColumn(
        "date", to_date(col(date_column))
    ).write.format("delta").mode("ignore").partitionBy("date").saveAsTable(
        ingestion_table
    )

    spark.read.format("bigquery").option("table", bq_table).load().withColumn(
        "date", to_date(col(date_column))
    ).filter(f"date >= date_sub(current_date(), {days})").createOrReplaceTempView(
        ingestion_view
    )
    spark.table(ingestion_view).write.format("delta").mode("overwrite").partitionBy(
        "date"
    ).option("replaceWhere", f"date >= date_sub(current_date(), {days})").option(
        "mergeSchema", "true"
    ).saveAsTable(
        ingestion_table
    )

    spark.table(ingestion_table).dropDuplicates().createOrReplaceTempView(history_view)

    spark.table(history_view).write.format("delta").partitionBy("date").mode(
        "ignore"
    ).saveAsTable(table)

    ingestion_updates = spark.sql(
        f"select * from {ingestion_table} where date >= date_sub(current_date(), {days})"
    )

    updates = ingestion_updates.dropDuplicates()

    updates.write.format("delta").mode("overwrite").partitionBy("date").option(
        "replaceWhere", f"date >= date_sub(current_date(), {days})"
    ).option("mergeSchema", "true").saveAsTable(table)
