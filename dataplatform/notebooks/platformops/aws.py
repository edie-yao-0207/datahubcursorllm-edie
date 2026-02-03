# MAGIC %run /backend/platformops/orderhistory/classes

# COMMAND ----------

from base64 import b64encode
from datetime import datetime
import json
import logging
from typing import Union

import boto3
from botocore.exceptions import ClientError
import pytz

US_AWS_REGION = "us-west-2"
EU_AWS_REGION = "eu-west-1"

# number of bytes we can to sqs at a time rounded down for safety
sqs_byte_len_limit = 260000

logger = logging.getLogger(__name__)


def get_current_aws_region():
    region = boto3.session.Session().region_name
    return region if region else US_AWS_REGION


def get_s3_body_str(s3obj: dict) -> str:
    return s3obj["Body"].read().decode("utf-8")


def s3get(s3, bucket: str, key: str) -> dict:
    try:
        resp = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"s3://{bucket}/{key} file not found")
            return None
        else:
            raise ex

    return resp


def get_s3_timestamp(s3, bucket: str, key: str) -> datetime:
    resp = s3get(s3, bucket, key)
    if resp is None:
        return None
    timestamp = float(get_s3_body_str(resp))
    return datetime.fromtimestamp(timestamp, pytz.utc)


def put_s3_timestamp(s3, bucket: str, key: str, d: datetime):
    s3.put_object(
        Body=f"{d.timestamp()}",
        Bucket=bucket,
        Key=key,
        ACL="bucket-owner-full-control",
    )


def send_sqs_msg_batch(sqs, queue_url: str, msgs_chunk: [str]):
    if len(msgs_chunk) == 0:
        return
    try:
        sqs.send_message_batch(QueueUrl=queue_url, Entries=msgs_chunk)
    except sqs.exceptions.InvalidMessageContents:
        logger.error(f"Message contents are invalid. Messages: {msgs_chunk}")


def send_sqs_msgs(sqs, queue_url: str, msgs: [dict]):
    batch_byte_len = 0
    batch = []

    for msg in msgs:
        msg_byte_len = len(json.dumps(msg).encode("utf-8"))
        if msg_byte_len > sqs_byte_len_limit:
            # TODO: @nateChandler fire off an alert if we get a single
            # order that is too large to send to SQS. For now, lets
            # just write it.
            print(msg)
            continue

        batch_byte_len += msg_byte_len

        # If we are going to be over the limit after adding this req,
        # go ahead and send it
        if batch_byte_len > sqs_byte_len_limit or len(batch) >= 10:
            send_sqs_msg_batch(sqs, queue_url, batch)
            batch.clear()
            batch_byte_len = msg_byte_len

        batch.append(msg)

    send_sqs_msg_batch(sqs, queue_url, batch)


def get_base_64_json(record: Union[Order, Exchange]) -> str:
    message_body = json.dumps(record.to_dict())

    return b64encode(message_body.encode("utf-8"))


def send_records_to_sqs(records: Union[Order, Exchange], url: str):
    base64_records = [get_base_64_json(record) for record in records]
    msgs = [
        {"MessageBody": str(base64_record, "utf-8"), "Id": str(i)}
        for i, base64_record in enumerate(base64_records)
    ]

    send_sqs_msgs(sqs, url, msgs)


def get_previous_run_latest_row_synced_time(s3_conn, bucket: str, key: str) -> datetime:
    return get_s3_timestamp(s3_conn, bucket, f"{key}")


def put_latest_row_synced_time(s3_conn, d: datetime, bucket: str, key: str):
    put_s3_timestamp(s3_conn, bucket, f"{key}", d)
