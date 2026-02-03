# IMPORTANT NOTE TO ENGINEER :
# If you change anything, please make run alerting_system_test_cases and ensure that all the tests to pass
# You must run the file on a dev cluster (not production cluster)

# This Notebook is responsible for sending slack alert notifications using slack bot

import os
from typing import List

# Import packages
import boto3
from slack import WebClient
from slack.errors import SlackApiError


# Gets the slack bot token stored in AWS SSM which is used to access the slack bot and send slack messages using the slack API
def _get_ssm_parameter(ssm_client: boto3.Session, name: str) -> str:
    res = ssm_client.get_parameter(Name=name, WithDecryption=True)
    return res["Parameter"]["Value"]


def _get_ssm_client() -> boto3.client:
    # Checks whether UC is enabled by checking the DATA_SECURITY_MODE.
    if os.environ.get("DATA_SECURITY_MODE") in ["SINGLE_USER", "USER_ISOLATION"]:
        boto_session = boto3.Session(
            botocore_session=dbutils.credentials.getServiceCredentialsProvider(
                "standard-read-parameters-ssm"
            )
        )
        return boto_session.client("ssm", region_name=boto_session.region_name)

    return boto3.client("ssm", region_name=boto3.session.Session().region_name)


def get_client() -> WebClient:
    slack_token = _get_ssm_parameter(
        ssm_client=_get_ssm_client(),
        name="SLACK_APP_TOKEN",
    )
    return WebClient(token=slack_token)


# Sends the message to specified slack channels using the slack API
# Returns the first slack channel that the slack bot failed to send messages to
def send_slack_message_to_channels(slack_msg: str, slack_channels: List[str]):
    slack_result_error_msg = None
    client = get_client()
    for cur_channel in slack_channels:
        try:
            response = client.chat_postMessage(
                channel=cur_channel,
                text=slack_msg,
            )
        except SlackApiError as e:
            if not slack_result_error_msg:
                slack_result_error_msg = cur_channel + " -- " + e.response["error"]
            else:
                slack_result_error_msg += (
                    "\n" + cur_channel + " -- " + e.response["error"]
                )
    return slack_result_error_msg


# Sends an image to the specified slack channels. The image should be specified via dbfs path
# ex: /dbfs/mnt/mounted_folder/directory/img.png
# The Alerts Bot must be a member of the channels you want to send the image to due to Slack
# permissions.
def send_image_to_channels(file_path: str, slack_channels: List[str]):
    client = _get_client()
    for cur_channel in slack_channels:
        try:
            client.files_upload_v2(
                file=file_path,
                channels=cur_channel,
            )
        except SlackApiError as e:
            return f"error uploading file: {e}"
