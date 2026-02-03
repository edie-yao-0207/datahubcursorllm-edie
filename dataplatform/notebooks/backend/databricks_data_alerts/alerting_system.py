# MAGIC %run ./alerting_system_alert_execution

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is the one users will import and it contains the function the users will call

# This is the user facing function. The user calls this function which kickoff the alerting system
def execute_alert(
    query_or_df: Union[str, pyspark.sql.dataframe.DataFrame],
    emails: List[str],
    slack_channels: List[str],
    alert_name: str,
    custom_message: str = None,
):
    return _execute_alert(
        query_or_df, emails, slack_channels, alert_name, custom_message
    )


print(
    """
Successfully imported Alerting System. Please use execute_alert(query_or_df, emails, alert_name, custom_message) to execute your alert.
query_or_df must be a spark dataframe (pyspark.sql.dataframe.DataFrame) or a SQL query in the form of a string only.
emails must of a list of strings only.
slack_channels must of a list of strings only.
alert_name can be any name you'd like to name the alert. It must be a string and only contain uppercase letters, lowercase letters, integers, underscores ( _ ), dash ( - ), and dots ( . ).
custom_message (optional) is a string which you'd like to include in the alert email or slack message.
"""
)
