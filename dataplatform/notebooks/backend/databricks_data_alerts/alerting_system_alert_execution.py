# MAGIC %run ./alerting_system_csv_generation

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is responsible for kicking off the alerting system

# logs the alert run information to a logs sql table in databricks
def _alert_logging(
    alert_input: AlertInput,
    csv_writing_result: WriteCSVtoS3Result,
    slack_result: SlackResult,
):
    schema = spark.table(ALERTS_LOG).schema
    new_row = namedtuple(
        "user_row",
        [
            "alert_name",
            "query",
            "emails",
            "alert_timestamp",
            "alert_execution_time",
            "row_count",
            "csv_url",
            "error_message",
            "slack_error_message",
        ],
    )
    df = spark.createDataFrame(
        [
            new_row(
                alert_input.alert_name,
                alert_input.query,
                alert_input.emails,
                alert_input.alert_timestamp,
                (dt.now() - alert_input.alert_timestamp).total_seconds(),
                csv_writing_result.row_count,
                csv_writing_result.url,
                csv_writing_result.error_message,
                slack_result.slack_error_message,
            )
        ],
        schema=schema,
    )
    df.write.mode("append").saveAsTable(ALERTS_LOG)


# Checks if the list value passed in contains only strings
def _check_email_string(emails: List[str]):
    for email in emails:
        if not isinstance(email, str):
            raise Exception("The emails argument must be a list of strings only.")


# Ensures that the alert name (alert_name) consists of only uppercase letters, lowercase letters, integers, underscores (_), dash(-), and dots (.).
def _check_alert_name(alert_name: str):
    if re.match(r"^[a-zA-Z0-9_\-\.]+$", alert_name) is None:
        raise Exception(
            f"The alert name {alert_name} must consist of only letters, numbers, underscores (_), hyphens (-), and periods (.)."
        )


# Ensures the values passed in are of the correct type
# Returns AlertInput object based on user provided values. This object is used throughout the program to keep track of the input
def _build_alert_instance(
    query_or_df: Union[str, pyspark.sql.dataframe.DataFrame],
    emails: List[str],
    slack_channels: List[str],
    alert_name: str,
    custom_message: str = None,
):
    _check_email_string(emails)
    _check_alert_name(alert_name)
    if not query_or_df:
        raise Exception("Query or Dataframe cannot be NULL/NONE")
    if isinstance(query_or_df, str):  # if query string is passed in
        return AlertInput(
            alert_name=alert_name,
            query=query_or_df,
            emails=emails,
            slack_channels=slack_channels,
            custom_message=custom_message,
            alert_timestamp=dt.now(),
        )
    if isinstance(
        query_or_df, pyspark.sql.dataframe.DataFrame
    ):  # if spark dataframe is passed in
        return AlertInput(
            alert_name=alert_name,
            query=None,
            emails=emails,
            slack_channels=slack_channels,
            custom_message=custom_message,
            alert_timestamp=dt.now(),
        )
    raise Exception(
        "Please pass in a Query as a string or a Spark Dataframe as pyspark.sql.dataframe.DataFrame only"
    )


# First, gets the AlertInput which is used throughout the program to keep track of the user input
# Then writes the alert data to S3 as CSV
# Then sends slack notification and email notification
# Logs the alert session information
# Returns True if alert executed succesffully, False if error occured in alert execution
def _execute_alert(
    query_or_df: Union[str, pyspark.sql.dataframe.DataFrame],
    emails: List[str],
    slack_channels: List[str],
    alert_name: str,
    custom_message: str = None,
):
    alert_input = _build_alert_instance(
        query_or_df, emails, slack_channels, alert_name, custom_message
    )
    csv_writing_result = _generate_csv(alert_input, query_or_df)
    slack_result = _send_slack_notification(alert_input, csv_writing_result)
    _send_email_notification(alert_input, csv_writing_result, slack_result)
    _alert_logging(alert_input, csv_writing_result, slack_result)
    return not csv_writing_result.error_message
