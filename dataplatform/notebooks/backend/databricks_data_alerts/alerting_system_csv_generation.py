# MAGIC %run ./alerting_system_slack_bot_messaging

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is responsible for generating the CSV and storing it on S3

# This function writes the alert output to S3 and gets a downloadable link for it
# First, it checks if a query or dataframe is passed in. If a query is passed in, it gets the output dataframe to peform the alert on
# Then, writes the partition file of the alert result to S3 using spark.write if dataframe is not empty
def _generate_csv(
    alert_input: AlertInput, df_or_query: Union[pyspark.sql.dataframe.DataFrame, str]
):
    alerting_df = df_or_query
    if isinstance(alerting_df, str):  # if query is passed in, get the output dataframe
        try:
            alerting_df = spark.sql(alert_input.query)  # execute query
        except Exception as e:  # If the query errored
            return WriteCSVtoS3Result(error_message=str(e))
    row_count = alerting_df.count()
    if row_count > 0:
        prefix = f"{S3_KEY_RESULTS}/{alert_input.alert_name}-{alert_input.alert_timestamp.strftime('%Y-%m-%d-%H-%M-%S')}"
        try:
            alerting_df.limit(CSV_LIMIT).coalesce(1).write.format("csv").option(
                "header", "true"
            ).save(f"s3://{S3_BUCKET}/{prefix}")
        except Exception as e:
            return WriteCSVtoS3Result(error_message=str(e))
        file_in_s3_location = get_s3_client(
            "samsara-databricks-playground-read"
        ).list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)["Contents"]
        for file in file_in_s3_location:
            cur_filename = file["Key"].split("/")[-1]
            if cur_filename.startswith("part-"):
                return WriteCSVtoS3Result(
                    alert_csv_filename=cur_filename,
                    row_count=row_count,
                    row_count_exceeds_limit=row_count > CSV_LIMIT,
                    url=f"https://s3.internal.samsara.com/s3/{S3_BUCKET}/{prefix}/{cur_filename}",
                )
