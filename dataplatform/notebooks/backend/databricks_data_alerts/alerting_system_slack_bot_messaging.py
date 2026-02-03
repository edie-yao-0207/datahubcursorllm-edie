# MAGIC %run ./alerting_system_emailing

# COMMAND ----------

# MAGIC %run ./slack_utility

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is responsible for sending slack alert notifications using slack bot


# Sends the message to specified slack channels using the slack API
# Returns the first slack channel that the slack bot failed to send messages to
def _send_slack_message_to_channels(slack_msg: str, alert_input: AlertInput):
    msg = slack_msg
    if alert_input.custom_message is not None:
        msg = alert_input.custom_message

    slack_message = send_slack_message_to_channels(
        slack_msg=msg, slack_channels=alert_input.slack_channels
    )
    return SlackResult(slack_error_message=slack_message)


# Constructs the slack message to send
# Then sends the slack message
# The slack message will contain the following information:
# 1. Alert name
# 2. Timestamp when alert was triggered (UTC)
# 3. Number of rows returned in output CSV (if alert was successful)
# 4. Link to download CSV output (if alert was successful)
# 5. Error message (if there was an error when executng the alert)
# 6. Query to trigger alert
# 8. Any custom message the user specifies
# Returns the first slack channel that the slack bot failed to send messages to
def _send_slack_notification(
    alert_input: AlertInput, csv_writing_result: WriteCSVtoS3Result
):
    slack_template = Template(SLACK_MESSAGE_TEMPLATE)
    slack_msg = slack_template.render(
        alert_input=alert_input,
        write_CSV_to_S3_result=csv_writing_result,
        alert_timestamp=alert_input.alert_timestamp,
    )
    if csv_writing_result.row_count is None:
        slack_msg_returned = _send_slack_message_to_channels(slack_msg, alert_input)
    elif csv_writing_result.row_count > 0:
        slack_msg_returned = _send_slack_message_to_channels(slack_msg, alert_input)
    else:
        slack_msg_returned = None

    return slack_msg_returned
