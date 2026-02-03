# MAGIC %run ./alerting_system_logs_table_creation

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This notebook is responsible for sending email alerts using Amazon SES

# Sends an email containg msg to all emails in to_emails using AWS SES service
def _ses_email_send(msg: MIMEMultipart, to_emails: List[str]):
    ses = get_ses_client("us-west-2")
    ses.send_raw_email(
        Source=msg["From"], Destinations=to_emails, RawMessage={"Data": msg.as_string()}
    )


# Prepares the email body along with from, to, and subject information in the right format in order to send the email
def _prepare_email(
    from_email: str,
    to_emails: List[str],
    subject: str,
    body_html: str,
    attachments=[],
    cc=[],
    bcc=[],
):
    attachment_ready_html = []
    for l in body_html:
        attachment_ready_html.append(l)
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = ", ".join(to_emails)
    body = MIMEText("\n".join(attachment_ready_html), "html")
    msg.attach(body)
    for raw_attachment in attachments:
        attachment = MIMEApplication(open(raw_attachment, "r").read())
        attachment.add_header(
            "Content-Disposition", "attachment", filename=raw_attachment
        )
        msg.attach(attachment)
    return msg


# Prepares the html body of the email (using a template)
# Then builds the email message from the html using _prepare_email
# Finally sends the email
# If error_msg is None, the alert was successful. Else, an error occured.
# The email will contain the following information:
# 1. Alert name
# 2. Timestamp when alert was triggered (UTC)
# 3. Number of rows returned in output CSV (if alert was successful)
# 4. Link to download CSV output (if alert was successful)
# 5. Error message (if there was an error when executng the alert)
# 6. Query to trigger alert
# 7. Error message regarding slack bot notifcation if sending alert to slack channels failed
# 8. Any custom message the user specifies
def _send_email_notification(
    alert_input: AlertInput,
    csv_writing_result: WriteCSVtoS3Result,
    slack_result: SlackResult,
):
    if len(alert_input.emails) == 0:
        return
    subject = alert_input.alert_name + " - Automated Data Alert"
    if csv_writing_result.error_message:
        subject = alert_input.alert_name + " - Automated Data Alert (ERROR)"

    if alert_input.custom_message is None:
        email_template = Template(HTML_EMAIL_TEMPLATE)
        html = [
            email_template.render(
                alert_input=alert_input,
                write_CSV_to_S3_result=csv_writing_result,
                slack_result=slack_result,
                alert_timestamp=alert_input.alert_timestamp,
            )
        ]
    else:
        html = alert_input.custom_message

    msg = _prepare_email(FROM_EMAIL, alert_input.emails, subject, html, attachments=[])
    _ses_email_send(msg, alert_input.emails)
