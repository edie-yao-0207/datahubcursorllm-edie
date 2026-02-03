# MAGIC %run ./sfdc_data_pull_funcs

# COMMAND ----------

# Views that store the samnumber accuracy calculation
metric_views = [
    "platops.samnumber_accuracy_name_match",
    "platops.samnumber_accuracy_serial_match",
    "platops.samnumber_accuracy_email_match",
]


def update_accuracy_metrics():
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    run_sfdc_data_pull()

    # record metrics
    for view in metric_views:
        spark.sql(
            f"""
            INSERT INTO {view}_out
            SELECT current_date() as created_at, *  FROM {view};
            """
        )


update_accuracy_metrics()
