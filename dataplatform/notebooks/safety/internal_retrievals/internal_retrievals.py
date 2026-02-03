# This file provides an interface for enqueuing messages in
# datacollectioncreateinternalretrievalworker's input queue.
# The main function in this file is called by a daily Databricks job defined by the
# Data Science team to retrieve videos needed to validate event detection algorithms.

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from base64 import b64encode
import json
import logging
import time
from typing import List

import boto3

US_SQS_QUEUE = "https://sqs.us-west-2.amazonaws.com/781204942244/samsara_data_collection_create_internal_retrieval_queue"
EU_SQS_QUEUE = "https://sqs.eu-west-1.amazonaws.com/947526550707/samsara_data_collection_create_internal_retrieval_queue"

US_AWS_REGION = "us-west-2"

# We expect a maximum of 3k requests over the course of 3 hours (180 minutes) each day
# and 10 requests per SQS batch (30-second sleep time supports up to 3.6k requests every 3 hours).
US_SLEEP_TIME_SECS = 30
# EU sleep time needs to be longer than US because all transcoding is processed by the prod cell.
# We set the sleep time to 3 minutes (10 req / 3 min) to support 200 req/hr, the maximum request rate currently handled
# by a single retrievaltranscodeworker cell.
EU_SLEEP_TIME_SECS = 180
# The send_message_batch API allows a maximum of 10 messages per call.
SQS_BATCH_LIMIT = 10

# Set this to different levels to see different log info.
LOG_LEVEL = logging.INFO
FORMAT = "%(asctime)-15s %(message)s"
logger = logging.getLogger(__name__)


class InternalRetrievalRequest:
    """
    A class that contains the fields needed to request an internal video retrieval.
    """

    def __init__(
        self,
        org_id,
        vg_device_id,
        start_ms,
        end_ms,
        is_hyperlapse=None,
        is_low_res=None,
        camera_streams=None,
    ):
        self.org_id = org_id
        self.vg_device_id = vg_device_id
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.is_hyperlapse = is_hyperlapse
        self.is_low_res = is_low_res
        self.camera_streams = camera_streams


class InternalRetrievalsRequester:
    """
    A class to hold a set of AWS / boto3 resources that can be used to interact with AWS.
    """

    def __init__(self):
        session = boto3.session.Session()
        self.aws_region = session.region_name
        self.sqs = get_sqs_client("data-collection-sqs")

    def get_sqs_url(self):
        return US_SQS_QUEUE if self.aws_region == US_AWS_REGION else EU_SQS_QUEUE

    def get_sleep_time(self):
        return (
            US_SLEEP_TIME_SECS
            if self.aws_region == US_AWS_REGION
            else EU_SLEEP_TIME_SECS
        )

    def write_messages_to_sqs(
        self,
        internal_retrieval_requests: List[InternalRetrievalRequest],
    ):
        sqs_messages = []
        for index, request in enumerate(internal_retrieval_requests):
            message_body = json.dumps(
                {
                    "org_id": request.org_id,
                    "vg_device_id": request.vg_device_id,
                    "start_ms": request.start_ms,
                    "end_ms": request.end_ms,
                    "is_hyperlapse": request.is_hyperlapse,
                    "is_low_res": request.is_low_res,
                    "camera_streams": request.camera_streams,
                }
            )

            base_64_message_body = b64encode(message_body.encode("utf-8"))
            message = {
                "MessageBody": str(base_64_message_body, "utf-8"),
                "Id": str(index),
            }

            sqs_messages.append(message)

        sleep_time_secs = self.get_sleep_time()
        for i in range(0, len(sqs_messages), SQS_BATCH_LIMIT):
            sqs_messages_chunk = sqs_messages[i : i + SQS_BATCH_LIMIT]

            try:
                self.sqs.send_message_batch(
                    QueueUrl=self.get_sqs_url(), Entries=sqs_messages_chunk
                )
            except self.sqs.exceptions.InvalidMessageContents:
                logger.error(
                    f"Message contents are invalid. Messages: {sqs_messages_chunk}"
                )

            time.sleep(sleep_time_secs)


def setup_logger():
    logging.basicConfig(format=FORMAT)
    logger.setLevel(LOG_LEVEL)


def main(
    internal_retrieval_requests: List[InternalRetrievalRequest],
) -> None:
    setup_logger()

    requester = InternalRetrievalsRequester()
    requester.write_messages_to_sqs(internal_retrieval_requests)

    logger.info(
        "Finished enqueuing all internal video retrieval requests to datacollectioncreateinternalretrievalworker."
    )
