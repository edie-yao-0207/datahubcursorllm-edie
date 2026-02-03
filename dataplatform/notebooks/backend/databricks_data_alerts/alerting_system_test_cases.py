# MAGIC %run ./alerting_system

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# This notebook contains test cases for the alerting system
# Please run this when a change is made to the alerting system (run on dev cluster, not on production cluster)

# This functuon just generates a test instance of AlertInput for test_send_slack_message_to_channels_1 and test_send_slack_message_to_channels_2
def generate_test_alert(slack_channels):
    return AlertInput(
        alert_name="Test_Alert",
        emails=["kalyan.vejalla@samsara.com"],
        slack_channels=slack_channels,
        alert_timestamp=datetime(2020, 8, 11, 17, 53, 58, 39784),
    )


# Tests that messaging are sending to slack
def test_send_slack_message_to_channels_1():
    test_alert = generate_test_alert(["test-channel-data-alerts"])
    test_alert.alert_csv_filename = (
        test_alert.alert_name
        + "-"
        + str(test_alert.alert_timestamp.date())
        + "-"
        + str(test_alert.alert_timestamp.time()).replace(":", "-").replace(".", "-")
    )
    slack_msg = "test case for test_send_slack_message_to_channels_1"
    slack_result = _send_slack_message_to_channels(slack_msg, test_alert)
    assert slack_result.slack_error_message == None, (
        "Slack message did not actually send successfully though it was expected to send. The slack error message is: "
        + test_alert.slack_error_message
    )


# Tests that the right error messaging is returned when an incorrect channel name is passed for slack messaging
def test_send_slack_message_to_channels_2():
    test_alert = generate_test_alert(["#incorrect_channel"])
    test_alert.alert_csv_filename = (
        test_alert.alert_name
        + "-"
        + str(test_alert.alert_timestamp.date())
        + "-"
        + str(test_alert.alert_timestamp.time()).replace(":", "-").replace(".", "-")
    )
    slack_msg = "test case for test_send_slack_message_to_channels_2"
    slack_result = _send_slack_message_to_channels(slack_msg, test_alert)
    expected_result = "#incorrect_channel -- channel_not_found"
    assert slack_result.slack_error_message == expected_result, (
        "Either the slack message was successfully sent though it was expected to fail due to 'channel_not_found' or the slack messaging failed due to a reason other than 'channel_not_found'. The returned slack error message: "
        + slack_result
    )


test_send_slack_message_to_channels_2()

# Tests _check_email_string
def test_check_email_string_pass():
    emails = ["kalyan.vejalla@samsara.com", "example@samsara.com"]
    try:
        _check_email_string(emails)
    except Exception as e:
        print(
            "_check_email_string failed (indicating the list contains not just strings) though the input was a list of strings."
        )


# Tests _check_email_string
def test_check_email_string_fail():
    emails = ["kalyan.vejalla@samsara.com", 123]
    try:
        _check_email_string(emails)
        print(
            "_check_email_string was successful (indicating the list contains just strings) though the input contained items other than strings."
        )
    except Exception as e:
        pass


# Tests check_alert_name
def test_check_alert_name_pass():
    alert_name = "AaAa_123-456_blah.test"
    try:
        _check_alert_name(alert_name)
    except Exception as e:
        print(
            "the alert name was not accepted though the alert_name specified is a valid one"
        )


# Tests check_alert_name
def test_check_alert_name_fail():
    alert_name = "~AaAa_123-456_blah/test!"
    try:
        _check_alert_name(alert_name)
        print(
            "the alert name passed though the alert_name specified is not a valid one"
        )
    except Exception as e:
        pass


# Tests that an email sends correctly
def test_ses_email_send_pass():
    msg = MIMEMultipart()
    to_emails = ["example@samsara.com"]
    msg["Subject"] = "example subject"
    msg["From"] = FROM_EMAIL
    msg["To"] = ", ".join(to_emails)
    try:
        _ses_email_send(msg, to_emails)
    except Exception as e:
        print("email failed when it was expected to send successfully")


# Tests that an email fails correctly
# The email should fail because the from email does not have permissions to send emails through Amazon's SES service.
def test_ses_email_send_fail():
    msg = MIMEMultipart()
    to_emails = ["example@samsara.com"]
    msg["Subject"] = "example subject"
    msg["From"] = "example@samsara.com"
    msg["To"] = ", ".join(to_emails)
    try:
        _ses_email_send(msg, to_emails)
        print("email sent successfully when it was expected fail")
    except Exception as e:
        pass


# Testing alert failure caused by invalid SQL syntax
def test_alerting_system_fail_sql_error():
    query = '"blah"'
    emails = ["kalyan.vejalla@samsara.com"]
    alert_name = "test_case_alert_failure_sql_error"
    actual_result = execute_alert(
        query,
        emails,
        ["test-channel-data-alerts", "adac"],
        alert_name,
        "This Alert should return an error due to invalid SQL syntax",
    )
    expected_result = False
    assert (
        actual_result == expected_result
    ), "alert executed successfully though it was expected to have errored due to invalid SQL syntax"


# Testing alert failure caused by spark error (invalid writing spark df result to CSV)
def test_alerting_system_fail_spark_error():
    query = "select c.* from kinesisstats.osdincrementalcellularusage c left join productsdb.devices d5 on c.object_id = d5.id"
    emails = ["kalyan.vejalla@samsara.com"]
    alert_name = "test_case_alert_failure_spark_df_error"
    actual_result = execute_alert(
        query,
        emails,
        ["test-channel-data-alerts", "adac"],
        alert_name,
        "This Alert should return an error due to invalid Spark failure",
    )
    expected_result = False
    assert (
        actual_result == expected_result
    ), "alert executed successfully though it was expected to have errored due to spark error (invalid writing spark df result to CSV)"


# Testing alert succes when a query is passed in as a string
def test_alerting_query():
    query = "select c.date, c.org_id, c.object_type, c.object_id, c.time from kinesisstats.osdincrementalcellularusage c left join productsdb.devices d1 on c.object_id = d1.id"
    emails = ["kalyan.vejalla@samsara.com"]
    alert_name = "test_case_alert_success_query_string"
    actual_result = execute_alert(
        query,
        emails,
        ["test-channel-data-alerts", "adac"],
        alert_name,
        "This Alert should be successfull",
    )
    expected_result = True
    assert (
        actual_result == expected_result
    ), "alert errored though it was expected to be successful"


# Testing alert succes when a spark dataframe is passed in
def test_alerting_sparkdf():
    query = """with usage_per_org_this_week as (
    select 
      d.org_id, 
      sum(data_usage) as sum_data_usage 
     from dataprep_cellular.att_daily_usage i 
     join productsdb.devices d on 
       i.iccid = d.iccid 
     where 
       record_received_date >= date_add(current_date(), -7)
     group by d.org_id
   ),
  usage_per_org_last_week as (
    select 
      d.org_id, 
      sum(data_usage) as sum_data_usage 
     from dataprep_cellular.att_daily_usage i 
     join productsdb.devices d on 
       i.iccid = d.iccid 
     where 
       record_received_date >= date_add(current_date(), -14) and 
       record_received_date < date_add(current_date(), -7)
     group by d.org_id
   )
  select * 
  from usage_per_org_this_week t1
  where 
    ( sum_data_usage - 
        (
          select sum(sum_data_usage)
          from usage_per_org_last_week t2
          where t1.org_id = t2.org_id
        ) ) /
              (
                select sum(sum_data_usage)
                from usage_per_org_last_week t2
                where t1.org_id = t2.org_id
              ) * 100 < -10 """

    df = spark.sql(query)
    emails = ["kalyan.vejalla@samsara.com"]
    alert_name = "test_case_alert_success_spark_df"
    actual_result = execute_alert(
        df,
        emails,
        ["test-channel-data-alerts", "adac"],
        alert_name,
        "This Alert should be successfull",
    )
    expected_result = True
    assert (
        actual_result == expected_result
    ), "alert errored though it was expected to be successful"


test_send_slack_message_to_channels_1()
test_send_slack_message_to_channels_2()
test_check_email_string_pass()
test_check_email_string_fail()
test_check_alert_name_pass()
test_check_alert_name_fail()
test_ses_email_send_pass()
test_ses_email_send_fail()
test_alerting_system_fail_sql_error()
test_alerting_system_fail_spark_error()
test_alerting_query()
test_alerting_sparkdf()
