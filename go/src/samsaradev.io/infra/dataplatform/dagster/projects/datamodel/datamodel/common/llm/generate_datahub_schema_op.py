import boto3
from dagster import In, Nothing, Out, op

from .gen_datahub_schema import create_schema_file


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    retry_policy=None,
)
def export_datahub_schemas(context):
    from ...common.utils import get_datahub_env

    formatted_json = create_schema_file(include_all=True, env="prod")
    # store in S3 using the same formatted JSON
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Body=formatted_json,
        Bucket="samsara-datahub-metadata",
        Key=f"metadata/exports/{get_datahub_env()}/schemas/datahub.json",
    )
    return 0
