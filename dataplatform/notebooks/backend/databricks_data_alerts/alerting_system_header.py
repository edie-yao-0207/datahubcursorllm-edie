# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook contains the headers for the alerting system, including packages to install and import and also constants.

import os
import re
import time
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime as dt
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Union

import boto3
import pyspark.sql.dataframe

# Import packages
from jinja2 import Template
from pyspark.sql import Row
from slack import WebClient
from slack.errors import SlackApiError

# Constants used in alerting system
ALERTS_LOG = "databricks_alerts.alert_logs"  # the table to which the alert log information is written
FROM_EMAIL = "no-reply@492164655156.databricks-alerts.samsara.com"  # the from email which has access to AWS SES in order to send alert emails
S3_BUCKET = "samsara-databricks-playground"  # the s3 bucket where alerts are written to
S3_KEY_UNMERGED = "alerts/unmerged_files"  # where alert results are written before they are merged into 1 CSV and emailed as a url
S3_KEY_RESULTS = "alerts/results"  # where CSV results are written
S3_KEY_HISTORY = (
    "alerts/history"  # where CSV results are stored along with previous CSV results
)
AWS_REGION = "us-west-2"
CSV_LIMIT = 1000000

# The email body in HTML that is sent to users
HTML_EMAIL_TEMPLATE = """
{% set timestampStr = alert_timestamp|string() %}
<font face="Verdana">
{% if not write_CSV_to_S3_result.error_message %}
  <h2> Details for your alert on {{timestampStr.split(" ")[0]}} -- {{alert_input.alert_name}} </h2>
{% else %}
  <h2> Alert Error for {{alert_input.alert_name}} on {{timestampStr.split(" ")[0]}} </h2>
{% endif %}
<font size='+1'>
<br><b>Alert name: </b>{{alert_input.alert_name}}
{% if not write_CSV_to_S3_result.error_message %}
  <br><b>Timestamp when alert triggered (UTC): </b>{{timestampStr.split(" ")[0]}} at {{timestampStr.split(" ")[1]}}
  {% if not write_CSV_to_S3_result.row_count_exceeds_limit %}
    <br><b>Rows returned: </b>{{write_CSV_to_S3_result.row_count}}
  {% else %}
    <br><b>Rows returned: </b>Row limit exceeded, returned max of rows 1,000,000
  {% endif %}
  <br><b>Link to download CSV output: </b>{{write_CSV_to_S3_result.url}}
  {% if alert_input.query %}
    <br><b>Query to trigger alert: </b><pre>{{alert_input.query}} </pre>
  {% else %}
    <br><b>Query to trigger alert: </b>** CUSTOM SPARK DATAFRAME (NO QUERY) **
  {% endif %}
{% else %}
  <br><b>Timestamp when error occurred for alert (UTC): </b>{{timestampStr.split(" ")[0]}} at {{timestampStr.split(" ")[1]}}
  {% if alert_input.query %}
    <br><b>Query to trigger error: </b><pre>{{alert_input.query}} </pre>
  {% else %}
    <br><b>Query to trigger error: </b>** CUSTOM SPARK DATAFRAME (NO QUERY) **
  {% endif %}
  <br><b>Error message: </b><pre>{{write_CSV_to_S3_result.error_message}} </pre>
{% endif %}
{% if slack_result.slack_error_message %}
  <br><b>Error sending alert notification to Slack: </b><pre>{{slack_result.slack_error_message}} </pre>
{% endif %}
{% if alert_input.custom_message %}
  <br><br> {{alert_input.custom_message}} <br>
{% endif %}
<br><br><b>Questions? </b> Post to #ask-data-analytics or e-mail datascience@samsara.com </font>
"""

# The slack message that is sent to users
SLACK_MESSAGE_TEMPLATE = """
{% set timestampStr = alert_timestamp|string() %}
{%- if not write_CSV_to_S3_result.error_message -%}
  *Alert for {{alert_input.alert_name}} on {{timestampStr.split(" ")[0]}}*
{% else -%}
  *Alert Error for {{alert_input.alert_name}} on {{timestampStr.split(" ")[0]}}*
{% endif -%}
*Alert name:* {{alert_input.alert_name}}
{% if not write_CSV_to_S3_result.error_message -%}
  *Timestamp when alert occured (UTC):* {{timestampStr.split(" ")[0]}} at {{timestampStr.split(" ")[1]}}
  {% if not write_CSV_to_S3_result.row_count_exceeds_limit -%}
    *Rows returned:* {{write_CSV_to_S3_result.row_count}}
  {% else -%}
    *Rows returned:* Row limit exceeded, returned max of rows 1,000,000
  {% endif -%}
  *Link to download CSV output:* {{write_CSV_to_S3_result.url}}
  {% if alert_input.query -%}
    *Query to trigger alert: * `{{alert_input.query}}`
  {% else -%}
    *Query to trigger alert: * CUSTOM SPARK DATAFRAME (NO QUERY)
  {% endif -%}
{% else -%}
  *Timestamp when error occurred for alert (UTC):* {{timestampStr.split(" ")[0]}} at {{timestampStr.split(" ")[1]}}
  {% if alert_input.query -%}
    *Query to trigger error: * `{{alert_input.query}}`
  {% else -%}
    *Query to trigger error: * CUSTOM SPARK DATAFRAME (NO QUERY)
  {% endif -%}
  *Error message:* `{{write_CSV_to_S3_result.error_message}}`
{% endif -%}
{% if alert_input.custom_message -%}
  {{alert_input.custom_message}}
{% endif -%}
*Questions? Post to * _#ask-data-analytics_ *or e-mail* _datascience@samsara.com_
"""

# This dataclass is used to store the user provided informaton for the alert
@dataclass
class AlertInput:
    alert_name: str
    emails: List[str]
    slack_channels: List[str]
    query: str = None
    custom_message: str = None
    alert_timestamp: dt = None


# This dataclass details of the exuction of the alert
@dataclass
class SlackResult:
    slack_error_message: str = None


# This dataclass stores the result of the query and the csv and the details associated with them
@dataclass
class WriteCSVtoS3Result:
    alert_csv_filename: str = None
    row_count: int = None
    row_count_exceeds_limit: bool = False
    url: str = None
    error_message: str = None
