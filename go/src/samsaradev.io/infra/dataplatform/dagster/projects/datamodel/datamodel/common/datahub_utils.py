import ast
import hashlib
import html
import importlib
import inspect
import json
import os
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import reduce
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import boto3
import pandas as pd
import pytz
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from dagster import AssetKey, AssetSpec, Config, In, Nothing, Out, asset, op
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config as dbxconfig
from databricks.sdk.core import oauth_service_principal
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import create_embed_mcp
from datahub.emitter.rest_emitter import DataHubRestEmitter, DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import SubTypes
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL, SchemaField
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    AuditStampClass,
    BooleanTypeClass,
    BytesTypeClass,
    DatasetPropertiesClass,
    DateTypeClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MapTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TimeTypeClass,
)
from pyspark.sql import functions as F
from pyspark.sql.types import *
from sql_formatter.core import format_sql
from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner

from ..common.constants import DBT_PROJECT_NAMESPACES
from ..common.utils import (
    AWSRegions,
    get_datahub_env,
    get_dbx_oauth_credentials,
    get_dbx_token,
    get_dbx_warehouse_id,
    get_gms_server,
    get_non_empty,
    get_region,
    initialize_datadog,
    safe_get,
    send_datadog_count_metric,
    send_datadog_gauge_metric,
    send_datadog_metric,
)
from ..ops.datahub.constants import (
    all_dbs,
    biztech_dbs,
    datamodel_dbs,
    datapipelines_dbs,
    datastreams_dbs,
    non_domain_dbs,
    rds_dbs,
)

if get_datahub_env() == "prod":
    METRIC_PREFIX = "datahub"
else:
    METRIC_PREFIX = "datahub.dev"

token = os.getenv("DATAHUB_GMS_TOKEN")
if os.getenv("MODE") == "LOCAL_DATAHUB_RUN":
    gms_server = "http://localhost:8080"
else:
    gms_server = get_gms_server()

initialize_datadog()

database_filter = None  # for testing only (e.g. "growth"); MUST BE None in prod

# for local testing, we should only run the datamodel_core database
if os.getenv("MODE") == "LOCAL_DATAHUB_RUN" and not database_filter:
    database_filter = "datamodel"


def get_repos():
    from datamodel import defs

    return defs.get_repository_def()


def get_asset_specs_by_key() -> Dict[AssetKey, AssetSpec]:

    asset_specs_by_key = {}

    # Merge all AssetSpec objects from datamodel project by AssetKey
    from datamodel import defs as datamodel_defs

    datamodel_specs = {spec.key: spec for spec in datamodel_defs.get_all_asset_specs()}

    asset_specs_by_key.update(datamodel_specs)

    # Merge all AssetSpec objects from dataweb project by AssetKey
    from dataweb import defs as dataweb_defs

    # filter out asset specs that are in the datamodel project
    asset_specs_to_add = {
        spec.key: spec
        for spec in dataweb_defs.get_all_asset_specs()
        if spec.key not in datamodel_specs
    }

    asset_specs_by_key.update(asset_specs_to_add)

    return asset_specs_by_key


def scrape_datahub_op_name():
    stack = inspect.stack()
    for caller in stack:
        if (
            caller.filename.find("ops/metadata.py") != -1
            or caller.filename.find("ops/metadata_hourly.py") != -1
        ) and (
            caller.function.find("extract_") >= 0
            or caller.function.find("emit_") >= 0
            or caller.function.find("get_") >= 0
            or caller.function.find("create_") >= 0
            or caller.function.find("tag_") >= 0
            or caller.function.find("delete_") >= 0
        ):
            break

    return caller.function


def log_op_duration(op_start_time):
    op_duration = time.time() - op_start_time

    send_datadog_gauge_metric(
        metric=f"dagster.{METRIC_PREFIX}.job.run.duration",
        value=op_duration,
        tags=[f"op:{scrape_datahub_op_name()}"],
    )


def hash_object(obj: Any) -> str:
    if isinstance(obj, dict):
        obj_str = json.dumps(obj, sort_keys=True)
    else:
        try:
            # Convert the object's __dict__ to a JSON string
            obj_str = json.dumps(obj.__dict__, sort_keys=True)
        except TypeError:
            # If the object is not JSON serializable, fall back to a string representation
            obj_str = str(obj)

    # Hash the string representation of the object
    return hashlib.sha256(obj_str.encode()).hexdigest()


def run_graphql(
    graph, graphql_query, variables=None
) -> Tuple[dict, Optional[Exception]]:
    err = None
    try:
        if variables:
            result = graph.execute_graphql(graphql_query, variables=variables)
        else:
            result = graph.execute_graphql(graphql_query)
    except Exception as e:
        result = None
        err = e

    status = "success" if err is None else "failure"

    send_datadog_count_metric(
        metric=f"{METRIC_PREFIX}.gms.graphql_query", tags=[f"status:{status}"]
    )

    return result, err


def emit_event(
    receiver: Union[DatahubRestEmitter, DataHubGraph],
    event,
    event_type: Optional[Literal["mce", "mcp"]] = None,
    extra_tags: Optional[Dict[str, Any]] = None,
) -> Optional[Exception]:
    err = None
    start_time = time.time()
    try:
        if event_type == "mce":
            receiver.emit_mce(event)
        elif event_type == "mcp":
            receiver.emit_mcp(event)
        else:
            receiver.emit(event)
    except Exception as e:
        err = e

    duration = time.time() - start_time

    status = "success" if err is None else "failure"

    send_datadog_count_metric(
        metric=f"{METRIC_PREFIX}.gms.sdk_emit_event", tags=[f"status:{status}"]
    )

    event_tags = []
    if extra_tags:
        for k, v in extra_tags.items():
            event_tags.append(f"{k}:{v}")

    send_datadog_gauge_metric(
        metric=f"{METRIC_PREFIX}.gms.sdk_emit_event_duration",
        value=duration,
        tags=event_tags,
    )

    return err


def run_dbx_query(query: str, region: str = AWSRegions.US_WEST_2.value):
    DOMAIN = f"samsara-dev-{region}.cloud.databricks.com"

    databricks_token = get_dbx_token(region)
    if databricks_token:
        credentials_provider = None
    else:
        credentials_provider = lambda: oauth_service_principal(
            dbxconfig(host=f"https://{DOMAIN}", **get_dbx_oauth_credentials(region))
        )

    with sql.connect(
        server_hostname=DOMAIN,
        http_path=f"/sql/1.0/warehouses/{get_dbx_warehouse_id(region)}",
        access_token=databricks_token,
        credentials_provider=credentials_provider,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
    return result


def list_files_in_s3_bucket(
    context, bucket_name, prefix, lookback_days=365
) -> Optional[dict]:
    result = defaultdict(list)
    s3 = boto3.client("s3")

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                last_modified = obj["LastModified"]

                # Check if the file is newer than lookback window
                if datetime.now(last_modified.tzinfo) - last_modified < timedelta(
                    days=lookback_days
                ):
                    if (
                        "/2023-" in key
                        or "/2022-" in key
                        or "/2021-" in key
                        or "/2020-" in key
                        or "/2019-" in key
                        or "/2018-" in key
                    ):
                        continue

                    obj_data = s3.get_object(Bucket=bucket_name, Key=key)
                    result[key.split("/")[2] + "." + key.split("/")[3]].append(
                        json.loads(obj_data["Body"].read().decode("utf-8"))
                    )
        return dict(result)
    except NoCredentialsError:
        context.log.info(f"Credentials not available for {bucket_name}, {prefix}")
        return None


def get_recent_s3_objects(
    context, s3, bucket_name: str, prefix: str
) -> List[Dict[str, str]]:
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    # process was OOMing for dataprep.osdaccelerometer_event_traces due to file volume
    utc = pytz.UTC
    x_days_ago = utc.localize(datetime.utcnow() - timedelta(days=1))

    all_objects = []
    i = 0
    for page in pages:
        for obj in page.get("Contents", []):
            i += 1
            if i % 10000 == 0:
                context.log.info(
                    f"processed {i} S3 objects for {prefix} in {bucket_name}"
                )
            if obj["Key"].endswith(".json") and obj["LastModified"] > x_days_ago:
                all_objects.append(obj)

    return all_objects


def get_latest_merge_or_insert(
    context,
    s3: boto3.client,
    bucket_name: str,
    delta_table_prefix: str,
    value_to_find: Literal["partitions", "last_update"],
) -> Optional[str]:

    delta_log_prefix = f"{delta_table_prefix}/_delta_log"

    try:
        all_objects = get_recent_s3_objects(context, s3, bucket_name, delta_log_prefix)
        log_files = sorted(
            [(obj["Key"], obj["LastModified"]) for obj in all_objects],
            key=lambda x: x[1],
            reverse=True,
        )
    except Exception as e:
        context.log.error(e)
        context.log.error(
            f"unable to get all objects for {delta_log_prefix} in {bucket_name}"
        )
        return None

    log_files = [x[0] for x in log_files if x[0].endswith(".json")] or []

    if not log_files:
        return None

    partitions = None
    zero_partition_count = 0  # some partitioned files have non-partitioned commits
    # we will read up to 25 files to find the latest partitioned commit

    for latest_log_file in log_files:
        s3_object = s3.get_object(Bucket=bucket_name, Key=latest_log_file)

        for line in s3_object["Body"].iter_lines():
            try:
                json_data = json.loads(line.decode("utf-8"))
                if value_to_find == "last_update" and "commitInfo" in json_data:
                    if "operation" in json_data["commitInfo"] and json_data[
                        "commitInfo"
                    ]["operation"] in [
                        "WRITE",
                        "MERGE",
                        "UPDATE",
                        "CREATE OR REPLACE TABLE AS SELECT",
                        "CREATE TABLE AS SELECT",
                        "CREATE TABLE",
                        "COPY INTO",
                    ]:
                        latest_update = json_data["commitInfo"]["timestamp"]
                        return latest_update

                elif value_to_find == "partitions" and (
                    "metaData" in json_data
                    or "add" in json_data
                    or "commitInfo" in json_data
                ):
                    if (
                        "commitInfo" in json_data
                        and "operationParameters" in json_data["commitInfo"]
                    ):
                        partitions = (
                            json_data["commitInfo"]["operationParameters"]
                            .get("partitionBy", "[]")
                            .replace("\\", "")
                        )
                    elif "metaData" in json_data:
                        partitions = str(
                            json_data.get("metaData", {}).get("partitionColumns")
                        )
                    elif "add" in json_data:
                        partitions = str(
                            list(json_data.get("add", {}).get("partitionValues").keys())
                        )
                    else:
                        partitions = "[]"

                    partition_lst = ast.literal_eval(partitions)

                    if len(partition_lst) >= 1:
                        return partitions
                    else:
                        zero_partition_count += 1
                        if zero_partition_count >= 25:
                            return "[]"
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON line: {e}")

    if value_to_find == "partitions" and not partitions:
        return "[]"

    return None


def get_all_partitions_from_s3(context, s3, bucket_name, delta_table_prefix):
    partitions = {}
    paginator = s3.get_paginator("list_objects_v2")
    for result in paginator.paginate(Bucket=bucket_name, Prefix=delta_table_prefix):
        if "Contents" in result:
            for key in result["Contents"]:
                if (
                    key["Key"].endswith(".parquet")
                    and "date=" in key["Key"]
                    and key["Key"].startswith(delta_table_prefix + "/")
                ):

                    date = key["Key"].split("date=")[1].split("/")[0]

                    earliest_landing_time = min(
                        int(key["LastModified"].timestamp()),
                        partitions.get(date, 10e9),
                    )
                    partitions[date] = earliest_landing_time

    return partitions


def get_latest_schema(
    context, s3_client, bucket_name: str, delta_log_path: str
) -> Optional[Dict[str, str]]:

    delta_log_path = f"{delta_log_path}/_delta_log"

    # Get a list of objects in the Delta log directory
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=delta_log_path)

    utc = pytz.UTC
    if "osdaccelerometer_event_traces" in delta_log_path:
        x_days_ago = utc.localize(datetime.utcnow() - timedelta(days=1))
    else:
        x_days_ago = utc.localize(datetime.utcnow() - timedelta(days=365))

    # Collect all log files and sort by last modified time (descending)
    log_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and obj["LastModified"] > x_days_ago:
                log_files.append(obj)
    log_files.sort(key=lambda x: x["LastModified"], reverse=True)

    latest_schema = {}

    # If no schema in checkpoint files, read the latest JSON log file
    for obj in log_files:
        if obj["Key"].endswith(".json"):
            try:
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])

                for line in s3_object["Body"].iter_lines():
                    log_entry = json.loads(line.decode("utf-8"))
                    if "metaData" in log_entry:
                        latest_schema = log_entry["metaData"]["schemaString"]
                        return json.loads(latest_schema)
            except Exception as e:
                context.log.error(e)
                context.log.error(
                    f"unable to get schema from {obj['Key']} in {bucket_name}"
                )
                break

    return latest_schema


def get_file_size_from_crc(
    context, s3_client, bucket_name: str, delta_log_path: str
) -> Tuple[int, int]:

    delta_log_path = f"{delta_log_path}/_delta_log"

    # Get a list of objects in the Delta log directory
    paginator = s3_client.get_paginator("list_objects_v2")

    pages = paginator.paginate(Bucket=bucket_name, Prefix=delta_log_path)

    utc = pytz.UTC
    x_days_ago = utc.localize(datetime.utcnow() - timedelta(days=90))

    # Collect all log files and sort by last modified time (descending)
    log_files = []
    for page in pages:
        print(page)
        for obj in page.get("Contents", []):
            print(obj)
            if obj["Key"].endswith(".crc") and obj["LastModified"] > x_days_ago:
                log_files.append(obj)
    log_files.sort(key=lambda x: x["LastModified"], reverse=True)

    # If no schema in checkpoint files, read the latest JSON log file
    for obj in log_files:
        if obj["Key"].endswith(".crc"):

            s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])

            # read in file and replace true with True and false with False
            data = s3_object["Body"].read().decode("utf-8")

            data = data.replace("true", "True")
            data = data.replace("false", "False")
            data = data.replace("null", "None")

            # read in text to dict
            data = ast.literal_eval(data)

            # later we will get tableSizeBytes and numFiles here too
            table_size_in_bytes = data["tableSizeBytes"]

            num_files = data["numFiles"]

            if table_size_in_bytes and num_files:
                return table_size_in_bytes, num_files
            elif table_size_in_bytes:
                return table_size_in_bytes, None
            elif num_files:
                return None, num_files
            else:
                return None, None

    return None, None


def get_latest_schema_from_crc(
    context, s3_client, bucket_name: str, delta_log_path: str
) -> Optional[Dict[str, str]]:

    delta_log_path = f"{delta_log_path}/_delta_log"

    # Get a list of objects in the Delta log directory
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=delta_log_path)

    utc = pytz.UTC
    x_days_ago = utc.localize(datetime.utcnow() - timedelta(days=90))

    # Collect all log files and sort by last modified time (descending)
    log_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".crc") and obj["LastModified"] > x_days_ago:
                log_files.append(obj)
    log_files.sort(key=lambda x: x["LastModified"], reverse=True)

    # If no schema in checkpoint files, read the latest JSON log file
    for obj in log_files:
        if obj["Key"].endswith(".crc"):

            s3_object = s3_client.get_object(Bucket=bucket_name, Key=obj["Key"])

            # read in file and replace true with True and false with False
            data = s3_object["Body"].read().decode("utf-8")

            data = data.replace("true", "True")
            data = data.replace("false", "False")
            data = data.replace("null", "None")

            # read in text to dict
            data = ast.literal_eval(data)

            # later we will get tableSizeBytes and numFiles here too
            y = eval(data["metadata"]["schemaString"])

            return y


def convert_to_datahub_schema(schema_fields):
    # Base structure for DataHub schema
    datahub_schema = {
        "schemaName": "ExampleSchema",
        "platform": "urn:li:dataPlatform:generic",
        "version": 0,
        "fields": [],
        "hash": "",
        "platformSchema": {
            "tableSchema": json.dumps(
                schema_fields
            )  # Storing original schema for reference
        },
    }

    # Function to recursively handle field types
    def process_field(field):
        field_type = field["type"]
        # Check if the field type is a complex type (struct or array)
        if isinstance(field_type, dict):
            if "type" in field_type and field_type["type"] == "struct":
                data_type = {
                    "type": {
                        "com.linkedin.schema.RecordType": {
                            "fields": [process_field(f) for f in field_type["fields"]]
                        }
                    }
                }
            elif "type" in field_type and field_type["type"] == "array":
                data_type = {
                    "type": {
                        "com.linkedin.schema.ArrayType": {
                            "items": process_field(
                                {
                                    "name": "",
                                    "type": field_type["elementType"],
                                    "nullable": field["nullable"],
                                    "metadata": {},
                                }
                            )
                        }
                    }
                }
        else:
            # Handle simple types
            data_type = {
                "type": {
                    "com.linkedin.schema." + str(field_type).capitalize() + "Type": {}
                }
            }

        # Define the schema field for DataHub
        dh_field = {
            "fieldPath": field["name"],
            "description": field.get("metadata", {}).get(
                "doc", "No description available."
            ),
            "nativeDataType": (
                field_type if isinstance(field_type, str) else field_type["type"]
            ),
            "type": data_type,
            "nullable": field["nullable"],
            "recursive": False,
        }
        return dh_field

    # Convert each field in the schema
    for field in schema_fields:
        datahub_field = process_field(field)
        datahub_schema["fields"].append(datahub_field)

    return datahub_schema


def get_datahub_type_prior(
    context, field: Dict[str, Any]
) -> Union[SchemaFieldDataTypeClass, None]:
    """Maps Spark data types to DataHub SchemaFieldDataTypeClass types."""
    # Check if the type is a simple string or a more complex type
    if isinstance(field["type"], str):
        spark_type = field["type"].lower()
    elif isinstance(field["type"], dict):
        return field["type"].get("type")
    else:
        # For complex types, further processing might be needed
        context.log.warning(
            f"Unknown Complex type encountered for field {field['name']}: {field['type']}"
        )
        return None

    mapping = {
        "string": StringTypeClass(),
        "timestamp": DateTypeClass(),
        "date": DateTypeClass(),
        "byte": BytesTypeClass(),
        "short": NumberTypeClass(),
        "integer": NumberTypeClass(),
        "long": NumberTypeClass(),
        "float": NumberTypeClass(),
        "double": NumberTypeClass(),
        "boolean": BooleanTypeClass(),
        "array": ArrayTypeClass(),
        "decimal(27,2)": NumberTypeClass(),
        "decimal(38,7)": NumberTypeClass(),
        "decimal(38,9)": NumberTypeClass(),
        "struct": RecordTypeClass(),
        # TODO(Add more mappings here for other data types)
    }

    if spark_type.startswith("decimal"):
        return NumberTypeClass()
    elif spark_type.startswith("timestamp"):
        return DateTypeClass()

    return mapping.get(spark_type, None)


def describe_struct(field):
    """Recursively describe a structured field."""
    if "type" in field and isinstance(field["type"], dict):
        # It's a struct or an array
        if field["type"]["type"] == "struct":
            # Handle struct type recursively
            inner_fields = ", ".join(
                [f"{f['name']}:{describe_struct(f)}" for f in field["type"]["fields"]]
            )
            return f"struct<{inner_fields}>"
        elif field["type"]["type"] == "array":
            # Handle array type
            element_type = describe_struct({"type": field["type"]["elementType"]})
            return f"array<{element_type}>"
    else:
        # Simple types
        return field["type"]


def create_describe_dict(schema):
    """Create a dictionary simulating SQL DESCRIBE output from a PySpark schema."""
    describe_dict = {}
    for field in schema["fields"]:
        if isinstance(field["type"], dict):
            describe_dict[field["name"]] = describe_struct(field)
        else:
            describe_dict[field["name"]] = field["type"]
    return describe_dict


def create_schema_fields_prior(
    schema: Dict[str, Any], context
) -> List[SchemaFieldClass]:
    """Creates a list of SchemaFieldClass objects from a schema definition."""
    fields = []
    for field in schema.get("fields", []):
        datahub_type = get_datahub_type_prior(context, field)
        if datahub_type is not None and datahub_type not in ("struct", "array", "map"):
            schema_field = SchemaFieldClass(
                fieldPath=field["name"],
                type=SchemaFieldDataTypeClass(type=datahub_type),
                nativeDataType=field["type"],
                nullable=field.get("nullable", None),
                description=field.get("metadata", {}).get("comment", None),
            )
            fields.append(schema_field)
        else:

            if datahub_type == "struct":
                dhub_type = RecordTypeClass()
            elif datahub_type == "array":
                dhub_type = ArrayTypeClass()
            elif datahub_type == "map":
                dhub_type = MapTypeClass()
            else:
                dhub_type = StringTypeClass()

            schema_field = SchemaFieldClass(
                fieldPath=field["name"],
                type=SchemaFieldDataTypeClass(type=dhub_type),
                nativeDataType="string",
                nullable=field.get("nullable", None),
                description=field.get("metadata", {}).get("comment", None),
            )
            fields.append(schema_field)
    return fields


def create_schema_fields(schema: Any, context) -> List[SchemaFieldClass]:
    """Creates a list of SchemaFieldClass objects from a PySpark StructType schema."""
    fields = []

    if hasattr(schema, "fields"):
        field_list = schema.fields
    else:
        field_list = schema["fields"]

    for field in field_list:
        # Determine the type of the field, handling complex data types appropriately
        datahub_type = get_datahub_type(context, field.dataType)
        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            if isinstance(field.dataType, StructType):
                dhub_type = RecordTypeClass()
            elif isinstance(field.dataType, ArrayType):
                dhub_type = ArrayTypeClass()
            elif isinstance(field.dataType, MapType):
                dhub_type = MapTypeClass()
            else:
                dhub_type = StringTypeClass()  # Fallback if unknown

            schema_field = SchemaFieldClass(
                fieldPath=field.name,
                type=SchemaFieldDataTypeClass(type=dhub_type),
                nativeDataType=field.dataType.simpleString(),
                nullable=field.nullable,
                description=field.metadata.get("comment", None),
            )
            fields.append(schema_field)
        else:
            # Simple types are directly translated
            schema_field = SchemaFieldClass(
                fieldPath=field.name,
                type=SchemaFieldDataTypeClass(type=datahub_type),
                nativeDataType=field.dataType.simpleString(),
                nullable=field.nullable,
                description=field.metadata.get("comment", None),
            )
            fields.append(schema_field)
    return fields


def get_datahub_type(context, pyspark_type):
    # Mapping PySpark data types to DataHub types (example mapping)
    mapping = {
        IntegerType: "INTEGER",
        StringType: "STRING",
        BooleanType: "BOOLEAN",
        LongType: "LONG",
        DoubleType: "DOUBLE",
        FloatType: "FLOAT",
        DateType: "DATE",
        TimestampType: "TIMESTAMP",
        StructType: "RECORD",
        ArrayType: "ARRAY",
        MapType: "MAP",
    }
    datahub_type = mapping.get(type(pyspark_type), StringTypeClass())
    context.log.warning(f"DataHub type: {datahub_type}, PySpark type: {pyspark_type}")
    if callable(datahub_type):
        datahub_type = datahub_type()
    return datahub_type


def get_datahub_type_for_crc(context, pyspark_type):
    # Mapping PySpark data types to DataHub types (example mapping)

    mapping = {
        "string": StringTypeClass(),
        "timestamp": TimeTypeClass(),
        "date": DateTypeClass(),
        "byte": BytesTypeClass(),
        "short": NumberTypeClass(),
        "integer": NumberTypeClass(),
        "long": NumberTypeClass(),
        "float": NumberTypeClass(),
        "double": NumberTypeClass(),
        "boolean": BooleanTypeClass(),
        "array": ArrayTypeClass(),
        "decimal(27,2)": NumberTypeClass(),
        "decimal(38,7)": NumberTypeClass(),
        "decimal(38,9)": NumberTypeClass(),
        "struct": RecordTypeClass(),
    }
    datahub_type = mapping.get(pyspark_type["type"], StringTypeClass())
    context.log.warning(f"DataHub type: {datahub_type}, PySpark type: {pyspark_type}")
    if callable(datahub_type):
        datahub_type = datahub_type()
    return datahub_type


def _simple_metric_query(source: str, dimensions: List[Dict[str, Any]]) -> str:
    query = f"SELECT * \nFROM {source}"
    args = []
    for dim in dimensions:
        dim_name = dim["name"]
        if dim_name == "date":
            args.append(
                f"-- filter to include dates between '2024-01-01' and '2024-01-01' and breakdown the result by each date.\n  date_range => ARRAY('2024-01-01', '2024-01-01', 'DAILY')"
            )
        elif dim_name == "org_id":
            args.append(
                f"-- filter to include all org_id values and breakdown the result by each org_id.\n  {dim_name} => ARRAY()"
            )
        elif dim_name == "device_type":
            args.append(
                f"-- filter to include only 'CM31' and 'CM32' values and breakdown the result by 'CM31' and 'CM32' groups.\n  {dim_name} => ARRAY('CM31', 'CM32')"
            )
        elif dim_name == "region":
            args.append(
                f"-- filter to include only 'eu-west-1' values and breakdown the result by 'eu-west-1' group.\n  {dim_name} => ARRAY('eu-west-1')"
            )
        else:
            args.append(
                f"-- filter to include all '{dim_name}' values and collaspe the result across all '{dim_name}' groups.\n  {dim_name} => NULL"
            )
    params = ",\n\n  ".join(args)
    query += f"(\n\n  {params}\n\n);"
    return query


def _metric_output_table(fields: List[str], values: List[str]) -> str:
    headers = [f"<th>{field}</th>" for field in fields]
    values = [f"<td><em>{value}</em></td>" for value in values]
    return f"""<table>
        <thead><tr>
        {''.join(headers)}
        </tr></thead>
        <tbody><tr>
        {''.join(values)}
        </tr></tbody>
        </table>"""


def generate_metric_description(context, database, table, metadata) -> str:

    catalog = "default"
    metrics_api_database = "metrics_api"
    expression_description = metadata["expression"]["metadata"]["comment"]
    query = f"DESCRIBE FUNCTION EXTENDED {catalog}.{metrics_api_database}.{table}"

    try:
        result = run_dbx_query(query)
        inputs = result[2 : len(metadata["dimensions"]) + 2]
        returns = result[
            len(metadata["dimensions"]) + 2 : (2 * len(metadata["dimensions"])) + 3
        ]
    except Exception as e:
        context.log.error(
            f"Failed to describe function for '{metrics_api_database}.{table}' \n {e}"
        )
        inputs = []
        returns = []
        return None

    dimensions = metadata["dimensions"]
    query = _simple_metric_query(f"{metrics_api_database}.{table}", dimensions)

    header = f"""<p><code>{database}.{table}</code> is a dataset that is used to compute {expression_description.lower()}
        \nThis is an unaggregated VIEW of data that has been cleansed with the necessary preprocessing for computing the <code>{table}</code> metric. You may query this view to explore the dataset but it does not filter or aggregate data into a specific result automatically. To leverage <code>{database}.{table}</code> you must manually apply the correct filtering and aggregation logic to your query to generate a correct result.
        \nWe provide a SQL-based API for you to use instead that handles the correct filtering and aggregation logic automatically for generating correct results.
        To retrieve data for the <code>{table}</code> metric using the SQL-based API, see <em>Using The Metrics SQL API</em> below.
    </p>"""

    api_header = "<h3>Using The Metrics SQL API</h3> \n\n"
    api_header += f"""<p>The Metrics SQL API uses <a href='https://www.databricks.com/blog/2021/10/20/introducing-sql-user-defined-functions.html'>User Defined Table Functions (UDTFs)</a> to serve you metrics from the Metrics Repo.
        You can think of these UDTFs like SQL VIEWS that accept optional input parameters, where the input parameters can modify the output results. All of the necessary filtering, grouping and aggregating is handled by the UDTF.
        \nThe Metrics SQL API provides a UDTF for every metric for you to query from. The name of the UDTF matches the name of the metric. The data for a metric is obtained by running a SQL query on the metric's UDTF.
        In your query, you will reference the metric's UDTF and provide optional input parameters (see example below). There is an optional input parameter for each dimension of the metric, and these will determine the filtering and break-down of the result.
        All metric UDTFs can be found in the <code>{metrics_api_database}</code> database under the same name (e.g., {metrics_api_database}.{table}). \n\n"""
    api_header += "The values that you give for the input parameters must be one of the following: \n\n"
    api_header += "- <strong>NULL</strong> \- A NULL parameter value will cause the result to include all rows of the dimension and to aggregate across all groups. In other words, a NULL value will exclude the dimension from the GROUP BY. \n"
    api_header += "- <strong>ARRAY()</strong> \- An ARRAY() parameter value (i.e., empty array) will cause the result to include all rows of the dimension and to break-down the aggregation by each group. In other words, an ARRAY() value will include the dimension in the GROUP BY. \n"
    api_header += "- <strong>ARRAY('a','b','c')</strong> or <strong>ARRAY('1','2','3')</strong> \- An ARRAY('a','b','c') (i.e., sliced-array) parameter value will cause the result to include only rows with values 'a' or 'b' or 'c' and to break-down the aggregation by groups 'a', 'b' and 'c'. In other words, an ARRAY('a','b','c') will only include the groups 'a', 'b' and 'c' in the GROUP BY. \n"
    api_header += "- <strong>ARRAY('a','b','c','COLLAPSED')</strong> or <strong>ARRAY('1','2','3','COLLAPSED')</strong> \- An ARRAY('a','b','c','COLLAPSED') (i.e., sliced-array) parameter value will cause the result to include only totals for the groups with values 'a' or 'b' or 'c' (no break-down). In other words, an ARRAY('a','b','c','COLLAPSED') takes the sum total of only including the groups 'a', 'b' and 'c' in the GROUP BY. \n\n"
    api_header += f"*<em>See</em> <a href='https://datahub.internal.samsara.com/dataset/urn:li:dataset:(urn:li:dataPlatform:databricks,{database}.{table},PROD)/Queries?is_lineage_mode=false'>Queries</a> <em> for examples.</em></p>"
    api_sub_header = f"<h5><strong>Metric UDTF:</strong> <code>{metrics_api_database}.{table}</code></h5>"
    api_sub_header += "\n\n<em>Example</em> - <a href='https://samsara-dev-us-west-2.cloud.databricks.com/sql/editor/c2c9f96e-b62c-4ba8-b217-275baacb16cf?o=5924096274798303'>Try it Yourself</a> \n\n"
    api_sub_header += f"<pre><code class='language-sql'>{query}</code></pre> \n  "
    api_docstring = "<br><br><br><strong>Input Parameters:</strong> "
    api_docstring += f"<pre><code class='language-sql'> {inputs[0].function_desc.replace('Input:', '').lstrip().replace('<', '&lt;').replace('>', '&gt;')} \n\n "
    api_docstring += (
        "\n\n ".join(
            [
                row.function_desc.lstrip().replace("<", "&lt;").replace(">", "&gt;")
                for row in inputs[1:]
            ]
        )
        + "</code></pre>"
    )
    api_docstring += "\n\n <br><strong>Output Result:</strong> \n\n"
    try:
        fields, dtypes, comments = zip(
            *[
                row.function_desc.replace("Returns:", "").split(maxsplit=2)
                for row in returns
            ]
        )
    except Exception as e:
        context.log.error(
            f"Failed to parse return values for {metrics_api_database}.{table} \n{e}"
        )
    api_docstring += _metric_output_table(fields, dtypes)
    return " \n\n ".join([header, api_header, api_sub_header, api_docstring])


def get_emitter() -> DataHubRestEmitter:
    USE_REST_EMITTER = True
    if USE_REST_EMITTER:
        return DataHubRestEmitter(
            gms_server=gms_server,
            extra_headers={"Authorization": f"Bearer {token}"},
        )


def get_graph(context) -> DataHubGraph:
    context.log.info(f"Getting graph for {gms_server}")
    graph = DataHubGraph(
        config=DatahubClientConfig(
            server=gms_server,
            extra_headers={"Authorization": f"Bearer {token}"},
        )
    )

    return graph


def get_db_and_table(
    urn: str, return_type: str = "tuple"
) -> Tuple[Optional[str], Optional[str], str]:
    x = urn.split(",")[1].split(".")
    if len(x) == 1:
        result = None, None, x[0]
    elif len(x) == 2:
        result = None, x[0], x[1]
    elif len(x) == 3:
        result = x[0], x[1], x[2]
    elif len(x) == 4 and x[0] in DBT_PROJECT_NAMESPACES:
        result = x[1], x[2], x[3]
    else:
        raise ValueError(f"unexpected urn format: {urn}")

    if return_type == "tuple":
        return result
    elif return_type == "string":
        return ".".join([x for x in result if x is not None])
    else:
        raise ValueError(f"unexpected return_type: {return_type}")


def get_platform(urn: str) -> str:
    """Extract platform from DataHub URN.

    Args:
        urn (str): DataHub URN string

    Returns:
        str: Platform name extracted from URN

    Examples:
        >>> get_platform("urn:li:dataset:(urn:li:dataPlatform:tableau,databricks.36070b15-36fb-d900-21e2-660b3e532999,PROD)")
        'tableau'
        >>> get_platform("urn:li:dataset:(urn:li:dataPlatform:databricks,product_analytics_staging.agg_daily_osdcommandschedulerstats,PROD)")
        'databricks'
        >>> get_platform("urn:li:dataset:(urn:li:dataPlatform:dbt,product_analytics_staging.agg_daily_osdcommandschedulerstats,PROD)")
        'dbt'
    """
    return urn.split(",")[0].split(":")[-1]


def get_all_urns(context, entity_type="DATASET") -> List[dict]:
    """
    Fetch all URNs using scrollAcrossEntities API for deep pagination beyond 10k.
    See: https://docs.datahub.com/docs/api/graphql/graphql-best-practices/

    Returns list of dicts: [{"entity": {"urn": "..."}}, ...]
    """
    graph = get_graph(context)
    page_size = 1000  # Reasonable batch size for scroll API
    urns = []
    scroll_id = None

    while True:
        # Build input fields list, then join with commas
        input_fields = [
            f"types: [{entity_type}]",
            'query: "*"',
            f"count: {page_size}",
        ]
        if scroll_id:
            input_fields.insert(0, f'scrollId: "{scroll_id}"')

        input_str = ", ".join(input_fields)

        scroll_query = f"""
            {{
            scrollAcrossEntities(input: {{ {input_str} }}) {{
                nextScrollId
                count
                total
                searchResults {{
                    entity {{
                        urn
                    }}
                }}
            }}
            }}
        """

        result, err = run_graphql(graph, scroll_query)
        if err:
            context.log.error(f"Error in scrollAcrossEntities: {err}")
            break

        if not result:
            context.log.error("scrollAcrossEntities returned empty result")
            break

        scroll_data = result.get("scrollAcrossEntities", {})
        page_results = scroll_data.get("searchResults", [])
        total = scroll_data.get("total", 0)
        next_scroll_id = scroll_data.get("nextScrollId")

        urns.extend(page_results)
        context.log.info(f"retrieved {len(urns)}/{total} urns")

        # Stop if no more results or no next page
        if not page_results or not next_scroll_id:
            break

        scroll_id = next_scroll_id

    context.log.info(f"retrieved {len(urns)} urns total")

    if database_filter and entity_type == "DATASET":
        urns = [x for x in urns if database_filter in x["entity"]["urn"]]

    context.log.info(f"found {len(urns)} filtered urns")

    return urns


def get_datahub_eks_s3_client() -> boto3.client:
    sts_client = boto3.client("sts")

    role_arn = "arn:aws:iam::492164655156:role/datahub-eks-nodes"

    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="datahub-eks-nodes-s3-session"
    )

    credentials = assumed_role_object["Credentials"]

    s3 = boto3.client(
        "s3",
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )

    return s3


def add_links(context, graph, dataset_urn, links_to_add):
    now = int(time.time() * 1000)  # milliseconds since epoch
    current_timestamp = AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")

    current_institutional_memory = graph.get_aspect(
        entity_urn=dataset_urn, aspect_type=InstitutionalMemoryClass
    )

    initial_institutional_memory_elements = (
        current_institutional_memory.elements.copy()
        if current_institutional_memory
        and hasattr(current_institutional_memory, "elements")
        else []
    )

    if current_institutional_memory and hasattr(
        current_institutional_memory, "elements"
    ):
        current_institutional_memory_elements = current_institutional_memory.elements
    else:
        current_institutional_memory_elements = []

    for description, link_to_add in links_to_add.items():

        new_institutional_memory_element = InstitutionalMemoryMetadataClass(
            url=link_to_add,
            description=description,
            createStamp=current_timestamp,
        )

        if current_institutional_memory:
            # new key
            if description not in [
                x.description for x in current_institutional_memory_elements
            ]:
                current_institutional_memory_elements.append(
                    new_institutional_memory_element
                )
            # update existing key
            elif link_to_add not in [
                x.url for x in current_institutional_memory_elements
            ]:
                new_elements = []

                for elem in current_institutional_memory_elements:
                    if elem.description != description:
                        new_elements.append(elem)

                current_institutional_memory_elements = new_elements

                current_institutional_memory_elements.append(
                    new_institutional_memory_element
                )
        else:
            # create a brand new institutional memory aspect
            current_institutional_memory = InstitutionalMemoryClass(
                elements=[new_institutional_memory_element]
            )

    # we want to preserve a consistent sort order regardless of when links are added to a table
    link_order = [
        "GitHub URL",
        "Dagster URL",
        "Databricks Job URL",
        "Data Contract",
        "Submit Feedback",
        "Glossary Term Details",
        "Dashboard",
    ]

    current_institutional_memory_elements_sorted = []

    for link in link_order:
        if link in links_to_add:
            for elem in current_institutional_memory_elements:
                if elem.description == link:
                    current_institutional_memory_elements_sorted.append(elem)

    if (
        current_institutional_memory_elements_sorted
        != initial_institutional_memory_elements
    ):
        current_institutional_memory.elements = (
            current_institutional_memory_elements_sorted
        )

        event = MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=current_institutional_memory,
        )
        emit_event(graph, event)


def transpose_single_row_scrollable(
    json_data, row_number=0, container_width="500px", char_limit=2000
):
    # Load the JSON data if it's a string, otherwise assume it's already a list of dictionaries
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data

    # Ensure the row number is valid
    if row_number >= len(data):
        return "Invalid row number"

    # Extract the specific row data
    row_data = data[row_number]

    # Start the scrollable container with inline CSS
    # html_output = f'<div style="width: {container_width}; overflow-x: auto;">'
    html_output = (
        f'<div style="width: {container_width}; overflow-x: scroll; overflow-y: auto;">'
    )

    # Start the table
    html_table = "<table>"

    # Add headers
    html_table += "<tr><th>Column</th><th>Value</th></tr>"

    # Add rows
    for key, value in row_data.items():
        if len(str(value)) > char_limit:
            value = str(value)[:char_limit] + "...[TRUNCATED]"
        html_table += f"<tr><td>{key}</td><td>{value}</td></tr>"

    # End the table
    html_table += "</table>"

    # Append table to the scrollable container and close the div
    html_output += html_table + "</div>"

    return html_output


def display_multiple_rows_scrollable(
    json_data, start_row=0, num_rows=10, container_width="500px", char_limit=75
):
    if isinstance(json_data, str):
        data = json.loads(json_data)
    else:
        data = json_data

    if start_row >= len(data):
        return "Invalid start row number"
    if start_row + num_rows > len(data):
        num_rows = len(data) - start_row

    column_headers = data[start_row].keys() if data else []

    html_output = (
        f'<div style="width: {container_width}; overflow-x: scroll; overflow-y: auto; height: auto;">'
        '<table style="width: 100%; table-layout: fixed;">'
    )

    # Adding headers
    html_table = "<tr>"
    for key in column_headers:
        html_table += f"<th style='max-width: {int(100/len(column_headers))}%; overflow-wrap: break-word;'>{key}</th>"
    html_table += "</tr>"

    # Adding data rows
    for i in range(start_row, start_row + num_rows):
        row_data = data[i]
        html_table += "<tr>"
        for key in column_headers:
            value = row_data.get(key, "")
            display_value = json.dumps(str(value)).strip('"')
            if len(display_value) > char_limit:
                display_value = display_value[:char_limit] + "...[TRUNCATED]"
            html_table += f"<td style='overflow-wrap: break-word; white-space: normal;'>{display_value}</td>"
        html_table += "</tr>"

    html_table += "</table></div>"
    html_output += html_table

    return html_output


def get_s3_objects(key: str):
    s3 = boto3.client("s3")

    obj_data = s3.get_object(
        Bucket="samsara-amundsen-metadata", Key=f"delta/{key}.json"
    )
    table_data_from_s3 = json.loads(obj_data["Body"].read().decode("utf-8"))

    if key not in ["teams", "users"]:
        table_data_from_s3_records = [
            x for x in table_data_from_s3.items() if x[0].split(".")[0] in all_dbs
        ]
        if database_filter:
            table_data_from_s3_records = [
                x for x in table_data_from_s3_records if database_filter in x[0]
            ]
    else:
        table_data_from_s3_records = table_data_from_s3

    return table_data_from_s3_records


def get_s3_preview_data(database: str, table: str):
    s3 = boto3.client("s3")

    obj_data = s3.get_object(
        Bucket="samsara-amundsen-metadata",
        Key=f"preview_data/{database}/{table}.json",
    )

    table_preview_data_from_s3 = json.loads(obj_data["Body"].read().decode("utf-8"))

    return table_preview_data_from_s3


# create a function for assessing high quality descriptions
def is_high_quality_description(description: str) -> bool:
    # assess semantic quality of the description
    description = description.lower()
    non_table_tokens = [
        x
        for x in description.replace("imported from", "")
        .replace("sourced from", "")
        .split(" ")
        if x and x.find(".") == -1
    ]
    if (
        description.find("imported from") == 0 or description.find("sourced from") == 0
    ) and len(non_table_tokens) < 3:
        return False
    elif len(description) < 10:
        return False

    return True


@dataclass
class DataRecord:
    dataset_urn: str
    has_schema: bool
    s3_location: str
    domain: str
    last_update: str
    table_type: str
    valid_glue_schema: bool
    num_assertions: int
    num_queries: int
    num_users: int
    table_description: str
    glossary_terms: List[str]
    merged_schema: Dict[str, Any]
    sql_sources_schema: Dict[str, Any]
    github_url: str
    fields_count: int
    fields_with_column_descriptions_count: int
    fields_with_quality_column_descriptions_count: int
    data_contract_url: str
    owner: str
    incident_status: str
    assertion_status: str
    tags: List[str]


def scrape_urn_metadata(context, graph, limit=-1) -> List[DataRecord]:

    records = []

    urns = get_all_urns(context)

    i = 0
    start_time = time.time()

    for urn in urns[:limit]:
        i += 1

        dataset_urn = urn["entity"]["urn"]

        current_schema_metadata = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=SchemaMetadataClass,
        )

        if hasattr(current_schema_metadata, "fields"):
            fields = current_schema_metadata.fields
        else:
            fields = []

        catalog, database, table = get_db_and_table(dataset_urn)

        current_properties = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=DatasetPropertiesClass,
        )

        if current_properties:
            s3_location = current_properties.get("customProperties", {}).get(
                "location",
                current_properties.get("customProperties", {}).get("storage_location"),
            )
            valid_glue_schema = current_properties.get("customProperties", {}).get(
                "valid_glue_schema"
            )
        else:
            s3_location = None
            valid_glue_schema = None

        query = """query getDataset($urn: String!) {
                        dataset(urn: $urn) {
                            ...nonSiblingDatasetFields
                        }
                        }

                        fragment nonSiblingDatasetFields on Dataset {
                        ...nonRecursiveDatasetFields
                        domain {
                            ...entityDomain
                        }
                        assertions(start: 0, count: 10) {
                            total
                        }
                            tags {
                                tags {
                                    tag {
                                        urn
                                    }
                                }
                            }
                            glossaryTerms {
                            terms {
                            term {
                                urn
                                glossaryTermInfo {
                                name
                            }
                            }
                            }
                        }
                        schemaMetadata {
                            version
                            fields {
                            fieldPath
                            nativeDataType
                            type
                            description
                            }
                        }
                        editableSchemaMetadata {
                            editableSchemaFieldInfo {
                            fieldPath
                            description
                            }
                        }
                        health {
                            ...entityHealth
                            __typename
                        }
                        assertions(start: 0, count: 10) {
                                total
                                __typename
                            }
                            operations(limit: 1) {
                                timestampMillis
                            lastUpdatedTimestamp
                        }
                        }

                        fragment entityHealth on Health {
                            type
                            status
                            message
                            causes
                            __typename
                        }

                        fragment nonRecursiveDatasetFields on Dataset {
                        urn
                        name
                        type
                        subTypes {
                            typeNames
                        }
                        lastIngested
                        properties {
                            name
                            customProperties {
                            key
                            value
                            }
                        }
                        editableProperties {
                            description
                        }
                        ownership {
                            ...ownershipFields
                        }
                        institutionalMemory {
                            ...institutionalMemoryFields
                        }
                        schemaMetadata {
                            ...schemaMetadataFields
                        }
                        statsSummary {
                            queryCountLast30Days
                            uniqueUserCountLast30Days
                        }
                        domain {
                            ...entityDomain
                        }
                        }

                        fragment ownershipFields on Ownership {
                        owners {
                            owner {
                            ... on CorpUser {
                                urn
                                username
                            }
                            ... on CorpGroup {
                                urn
                                name
                            }
                            }
                        }
                        lastModified {
                            time
                        }
                        }

                        fragment institutionalMemoryFields on InstitutionalMemory {
                        elements {
                            url
                            author {
                            urn
                            username
                            }
                            description
                        }
                        }

                        fragment schemaMetadataFields on SchemaMetadata {
                        version
                        aspectVersion
                        }

                        fragment entityDomain on DomainAssociation {
                        domain {
                            urn
                            type
                            properties {
                            name
                            description
                            }
                        }
                        }
"""

        variables = {
            "urn": dataset_urn,
        }

        result, err = run_graphql(graph, graphql_query=query, variables=variables)
        if err:
            context.log.error(err)
            continue

        if i % 100 == 0:
            context.log.info(f"processed {i} urns")
            # context.log.info(f"graphsql result: {result}")
            context.log.info(
                f"estimated time to complete = {(len(urns) / i) * (time.time() - start_time) / 60} minutes"
            )

        if (result or {}).get("dataset", {}).get("operations"):
            last_update = (
                result["dataset"].get("operations")[0].get("lastUpdatedTimestamp")
            )
        else:
            last_update = None
        if result["dataset"].get("domain"):
            domain = (
                result["dataset"]
                .get("domain", {})
                .get("domain", {})
                .get("properties", {})
                .get("name")
            )
        else:
            domain = None

        if result["dataset"].get("subTypes"):
            table_type = (
                result["dataset"]
                .get("subTypes", {"typeNames": [None]})
                .get("typeNames", [None])[0]
            )
        else:
            table_type = None

        if err:
            context.log.error(err)
            last_update = None
            domain = None
            table_type = None

        if len(fields) > 0:
            has_schema = True
        else:
            has_schema = False

        num_assertions = result["dataset"].get("assertions", {}).get("total", 0)

        table_description = (result["dataset"].get("editableProperties") or {}).get(
            "description"
        )

        num_queries = (result["dataset"].get("statsSummary") or {}).get(
            "queryCountLast30Days"
        )

        num_users = (result["dataset"].get("statsSummary") or {}).get(
            "uniqueUserCountLast30Days"
        )

        glossary_terms = [
            (((x or {}).get("term", {}) or {}).get("urn", {}) or {})
            for x in (result["dataset"].get("glossaryTerms", {}) or {}).get("terms", [])
        ]

        tags = [
            x.get("tag", {}).get("urn")
            for x in (result["dataset"].get("tags", []) or {}).get("tags", [])
        ]

        glossary_terms = [x for x in glossary_terms if x]

        schema_metadata = result["dataset"].get("schemaMetadata", {})
        editable_schema_metadata = result["dataset"].get("editableSchemaMetadata", {})

        merged_schema = {}
        fields_count = 0
        fields_with_column_descriptions = []
        fields_with_quality_column_descriptions = []

        if schema_metadata and "fields" in schema_metadata:
            for field in schema_metadata["fields"]:
                fields_count += 1
                merged_schema[field["fieldPath"]] = {
                    "nativeDataType": field["nativeDataType"],
                    "type": field["type"],
                    "originalDescription": field["description"],
                }

        if (
            editable_schema_metadata
            and "editableSchemaFieldInfo" in editable_schema_metadata
        ):
            for editable_field in editable_schema_metadata["editableSchemaFieldInfo"]:
                if editable_field["fieldPath"] in merged_schema:
                    merged_schema[editable_field["fieldPath"]][
                        "modifiedDescription"
                    ] = editable_field["description"]
                else:
                    # This handles the case where a new editable field is introduced without an existing schema definition.
                    merged_schema[editable_field["fieldPath"]] = {
                        "modifiedDescription": editable_field["description"]
                    }

        def _parse_complex_string_type(field: str) -> str:
            return ".".join(
                [x for x in field.split(".") if "[" not in x and "]" not in x]
            )

        for field in merged_schema:
            merged_schema[field]["mergedDescription"] = get_non_empty(
                merged_schema[field],
                "modifiedDescription",
                merged_schema[field].get("originalDescription", "") or "",
            )

            if len((merged_schema[field].get("mergedDescription", "") or "")) > 5:
                # dedupe mixture of simple and complex field names that can occur in prod graph
                fields_with_column_descriptions.append(
                    _parse_complex_string_type(field)
                )

            if is_high_quality_description(merged_schema[field]["mergedDescription"]):
                fields_with_quality_column_descriptions.append(
                    _parse_complex_string_type(field)
                )
            elif len(merged_schema[field]["mergedDescription"]) >= 5:
                context.log.warning(
                    f"field {field} has low quality description:\n {merged_schema[field]['mergedDescription']}"
                )

        sql_sources = [
            x
            for x in safe_get(result, ["dataset", "properties", "customProperties"], {})
            if x.get("key").startswith("spark.sql.sources.schema")
        ]

        sql_sources = {x.get("key"): x.get("value") for x in sql_sources}

        sql_sources_schema = {}

        if not sql_sources:
            sql_sources_schema = {}
        elif len(sql_sources) == 1 and "spark.sql.sources.schema" in sql_sources:
            sql_sources_schema = json.loads(sql_sources["spark.sql.sources.schema"])
        else:
            parts = sorted(
                list(
                    {
                        x: y
                        for x, y in sql_sources.items()
                        if "spark.sql.sources.schema" in x and "part." in x
                    }.items()
                )
            )

            full_str = ""
            for part in parts:
                full_str += part[1]

            try:
                sql_sources_schema = json.loads(full_str)
            except Exception as e:
                context.log.error(e)
                context.log.error(f"unable to parse sql_sources for {dataset_urn}")
                sql_sources_schema = {
                    "error": "more than 5 sql sources parts, cannot process"
                }

        institutional_memory = safe_get(
            result, ["dataset", "institutionalMemory", "elements"], {}
        )

        github_url = [
            x.get("url")
            for x in institutional_memory
            if x.get("description") == "GitHub URL"
        ]

        if github_url:
            github_url = github_url[0]
        else:
            github_url = None

        data_contract_url = [
            x.get("url")
            for x in institutional_memory
            if x.get("description") == "Data Contract"
        ]

        data_contract_url = None if not data_contract_url else data_contract_url[0]

        ownership = [
            x["owner"]["urn"]
            for x in safe_get(result, ["dataset", "ownership", "owners"], [])
        ]
        owner = None if not ownership else ownership[0]

        fields_with_column_descriptions_count = len(
            set(fields_with_column_descriptions)
        )

        fields_with_quality_column_descriptions_count = len(
            set(fields_with_quality_column_descriptions)
        )

        health = result["dataset"].get("health", [])
        incident_status = None
        assertion_status = None

        for health_item in health:
            if health_item.get("type") == "INCIDENTS":
                incident_status = health_item.get("status")
            elif health_item.get("type") == "ASSERTIONS":
                assertion_status = health_item.get("status")

        records.append(
            [
                dataset_urn,
                has_schema,
                s3_location,
                domain,
                last_update,
                table_type,
                valid_glue_schema,
                num_assertions,
                num_queries,
                num_users,
                table_description,
                glossary_terms,
                merged_schema,
                sql_sources_schema,
                github_url,
                fields_count,
                fields_with_column_descriptions_count,
                fields_with_quality_column_descriptions_count,
                data_contract_url,
                owner,
                incident_status,
                assertion_status,
                tags,
            ]
        )

    records = [DataRecord(*record) for record in records]

    return records


class LoadTestConfig(Config):
    num_tests: int = 10


def run_search_query(graph, search_term, context):
    start_time = time.time()
    result, err = run_graphql(
        graph,
        graphql_query=datahub_search_query,
        variables={
            "input": {
                "types": [],
                "query": search_term,
                "start": 0,
                "count": 10,
                "filters": [],
                "orFilters": [],
                "searchFlags": {"getSuggestions": True},
            }
        },
    )
    if err:
        context.log.error(err)

    end_time = time.time()
    duration = end_time - start_time

    metric = f"{METRIC_PREFIX}.search_duration_seconds"

    tags = [f"search_term:{search_term}"]

    # log to Datadog
    send_datadog_metric("distribution", duration, metric, tags, context=context)

    context.log.info(f"search took {duration} seconds")

    context.log.info(result["searchAcrossEntities"]["total"])


def chain_functions(func_list: List[Callable[..., Any]]) -> Any:
    """
    Chain a list of functions where each function takes the output of the previous one as its input.

    The first function in the list should not require any input parameters; it must generate the initial value.

    Parameters:
    func_list (list): A list of functions.

    Returns:
    any: The result of chaining the functions.
    """

    # Initialize by executing the first function (assuming it requires no arguments)
    initial_value = func_list[0]()

    # Apply the remaining functions in the list
    return reduce(lambda acc, func: func(acc), func_list[1:], initial_value)


def get_dbx_users(
    databricks_instance="samsara-dev-us-west-2",
) -> Tuple[List[Dict[str, Any]], str]:

    config = {"host": f"https://{databricks_instance}.cloud.databricks.com"}

    if get_datahub_env() == "dev":
        config["token"] = get_dbx_token(AWSRegions.US_WEST_2.value)
    else:
        oauth_credentials = get_dbx_oauth_credentials(AWSRegions.US_WEST_2.value)
        config["client_id"] = oauth_credentials["client_id"]
        config["client_secret"] = oauth_credentials["client_secret"]

    dbx_client = WorkspaceClient(**config)

    users = dbx_client.users.list()

    if users:
        return [user.as_dict() for user in users], None
    else:
        return [], "failed to get databricks users"


def load_highlighted_queries(directory: str) -> Dict[str, Dict[str, str]]:
    query_map = {}
    directory = os.path.abspath(
        directory
    )  # Ensure we have an absolute path for correct path operations
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(root, file)
                # Build the module key from the relative path to the file and remove the file extension
                relative_path = os.path.relpath(
                    path, directory
                )  # Get the relative file path from the directory
                module_key = os.path.splitext(relative_path)[0].replace(
                    os.sep, "."
                )  # Replace OS-specific path separators with '.'
                spec = importlib.util.spec_from_file_location(module_key, path)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                # Assuming each module has a 'queries' dictionary
                query_map[module_key] = module.queries
    return query_map


def get_s3_bucket_from_table(graph, urn):
    bucket_name = None
    delta_table_prefix = None
    s3_location = ""

    current_properties = graph.get_aspect(
        entity_urn=urn, aspect_type=DatasetPropertiesClass
    )

    if hasattr(current_properties, "customProperties"):
        # update to search for serde location
        s3_location = current_properties.customProperties.get(
            "location",
            current_properties.customProperties.get("storage_location"),
        )

    if not s3_location or not s3_location.startswith("s3://"):
        pass
    else:
        bucket_name = s3_location.replace("s3://", "").split("/")[0]
        delta_table_prefix = (
            s3_location.replace("s3://", "").replace(bucket_name, "").strip("/")
        )

    return bucket_name, delta_table_prefix


def emit_structured_properties(graph, dataset_urn, structured_properties):
    # get existing structured properties
    existing_structured_properties = graph.get_aspect(
        entity_urn=dataset_urn, aspect_type=StructuredPropertiesClass
    )
    # merge existing and new structured properties
    merged_structured_properties = {}

    if existing_structured_properties and existing_structured_properties.properties:
        for prop in existing_structured_properties.properties:
            merged_structured_properties[prop.propertyUrn.split(":")[-1]] = prop.values[
                0
            ]

    merged_structured_properties.update(structured_properties)

    mcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        entityUrn=dataset_urn,
        aspect=StructuredPropertiesClass(
            properties=[
                StructuredPropertyValueAssignmentClass(
                    propertyUrn=f"urn:li:structuredProperty:{prop_key}",
                    values=[prop_value],
                )
                for prop_key, prop_value in merged_structured_properties.items()
            ]
        ),
    )
    graph.emit_mcp(mcp)


def get_untrustworthy_tables():
    def _get_query(where_clause, table_clause, ddl_clause):

        query = f"""with t as (
                    select
                        user_identity.email as user,
                        REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            lower(request_params.commandText),
                            '\\\s+',
                            -- Matches any whitespace character (spaces, tabs, newlines) one or more times
                            ' ' -- Replace with a single space
                        ),
                        '[\r\n]+',
                        -- Matches one or more newline characters
                        ' ' -- Replace with a single space
                        ) as query,
                        unix_timestamp(event_time) as timestamp
                    from
                        system.access.audit
                    where
                        request_params.commandText is not null
                        and {where_clause}
                        and workspace_id = 5924096274798303
                    ),
                    t2 as (
                    select
                        *,
                        REGEXP_EXTRACT(query, '{table_clause}\\\s+([\\\w\\\.]+)\\\s+', 1) as extracted_table
                    from
                        t
                    where
                        query like '{ddl_clause}'
                    )
                    select
                    distinct extracted_table
                    from
                    t2
                    where
                    extracted_table like '%.%'
                    order by
                    1
            """

        return query

    # workspace_id 5924096274798303 is the US Databricks workspace (does not include EU queries)

    queries = [
        _get_query(
            where_clause="""service_name = 'notebook'
                    and user_identity.email like '%@samsara.com'
                    """,
            table_clause="table",
            ddl_clause="create or replace table%",
        ),
        _get_query(
            where_clause="""user_identity.email like '%@samsara.com'
                       and user_identity.email like '%dev-databricks%'
                    """,
            table_clause="merge into",
            ddl_clause="%merge into%",
        ),
    ]

    table_list_of_lists = []

    for query in queries:

        result = run_dbx_query(query)

        x = [record.extracted_table for record in result]

        table_list_of_lists.append(x)

    unverified_tables = table_list_of_lists[0]

    for table_list in table_list_of_lists[1:]:
        unverified_tables = [x for x in unverified_tables if x not in table_list]

    return unverified_tables


def sync_tags_or_terms(
    context, graph, tag_or_term_urn, entity_type, tbl_list, tables_in_graph
):
    tbl_list = [x for x in tbl_list if x in tables_in_graph]

    if entity_type == "GLOSSARY_TERM":
        query = """
            query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
                searchResults {
                entity {
                    urn
                    ... on GlossaryNode {
                    urn
                    children: relationships(
                        input: {types: ["IsPartOf"], direction: INCOMING, start: 0, count: 10000}
                    ) {
                                total
                            }
                            }
                        }
                    }
                }
            }
        """
    else:
        query = """
            query getSearchResultsForMultiple($input: SearchAcrossEntitiesInput!) {
                searchAcrossEntities(input: $input) {
                    searchResults {
                    entity {
                        urn
                    }
                    }
                }
                }
        """

    current_tagged_tables, err = run_graphql(
        graph,
        graphql_query=query,
        variables={
            "input": {
                "query": "*",
                "start": 0,
                "count": 10000,
                "orFilters": [
                    {
                        "and": [
                            {
                                "field": (
                                    "glossaryTerms"
                                    if entity_type == "GLOSSARY_TERM"
                                    else "tags"
                                ),
                                "values": [
                                    f"urn:li:{'glossaryTerm' if entity_type == 'GLOSSARY_TERM' else 'tag'}:{tag_or_term_urn}"
                                ],
                            }
                        ]
                    }
                ],
            }
        },
    )

    if current_tagged_tables:
        current_tagged_tables = [
            x["entity"]["urn"]
            for x in current_tagged_tables.get("searchAcrossEntities", {}).get(
                "searchResults", []
            )
        ]
    else:
        current_tagged_tables = []

    context.log.info(
        f"current_tagged_tables for {entity_type} and {tag_or_term_urn}: {current_tagged_tables}"
    )

    tables_to_untag = [
        x
        for x in current_tagged_tables
        if get_db_and_table(x, return_type="string") not in tbl_list
    ]

    context.log.info(f"tables_to_untag: {tables_to_untag}")
    context.log.info(f"tables_to_tag: {tbl_list}")

    for urn_to_untag in tables_to_untag:
        input = {"resourceUrn": urn_to_untag}

        if entity_type == "GLOSSARY_TERM":
            input["termUrn"] = f"urn:li:glossaryTerm:{tag_or_term_urn}"
        else:
            input["tagUrn"] = f"urn:li:tag:{tag_or_term_urn}"

        entity_type_str = "Term" if entity_type == "GLOSSARY_TERM" else "Tag"
        _, err = run_graphql(
            graph=graph,
            graphql_query=f"""
                mutation remove{entity_type_str}($input: {entity_type_str}AssociationInput!) {{
                    remove{entity_type_str}(input: $input)
                    }}
                """,
            variables={"input": input},
        )

        if err:
            context.log.error(err)

    for tbl in tbl_list:

        entity_type_str = "Terms" if entity_type == "GLOSSARY_TERM" else "Tags"

        input = {
            "resources": [
                {
                    "resourceUrn": f"urn:li:dataset:(urn:li:dataPlatform:databricks,{tbl},PROD)",
                    "subResourceType": None,
                }
            ],
        }

        if entity_type == "GLOSSARY_TERM":
            input["termUrns"] = [f"urn:li:glossaryTerm:{tag_or_term_urn}"]
        else:
            input["tagUrns"] = [f"urn:li:tag:{tag_or_term_urn}"]

        result, err = run_graphql(
            graph,
            graphql_query=f"""
            mutation batchAdd{entity_type_str}($input: BatchAdd{entity_type_str}Input!) {{
                    batchAdd{entity_type_str}(input: $input)
                    }}
            """,
            variables={"input": input},
        )

        if err:
            context.log.error(err)


def parse_table_from_submitted_query(code_to_parse: str) -> Optional[str]:
    if code_to_parse.find("(truncated)") > 0:
        return None

    table_name = None
    database = None
    text_strs_to_replace = {
        "'": "`",
        "--sql": "",
        "--endsql": "",
        "-- COMMAND ----------": "",
        "-- Databricks notebook source": "",
        "# COMMAND ----------": "",
    }

    for text_str in text_strs_to_replace:
        code_to_parse = code_to_parse.replace(text_str, "")

    spark_write_patterns = [
        r"\.write[\s\S]+?\.saveAsTable\(\s*\"([^\"]+)\"\s*\)",
        r"\.write[\s\S]+?\.insertInto\(\s*\"([^\"]+)\"\s*\)",
        r"DeltaTable\.forName\(spark,\s*\"([^\"]+)\"\)",
        r"create_or_update_table\([\s\S]*?\"([^\"]+)\"",
        r"merge into\s+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+)",
    ]

    tables = []

    for pattern in spark_write_patterns:
        matches = re.findall(
            pattern,
            code_to_parse,
            re.MULTILINE | re.DOTALL,
        )
        tables.extend(matches)

    if len(tables) > 0:
        last_table = tables[-1]
        if "." in last_table:
            database, table_name = last_table.split(".", 1)
        else:
            table_name = last_table

    sql_to_parse = code_to_parse

    # Remove lines that are comments
    sql_lines = sql_to_parse.split("\n")
    clean_sql_lines = [
        line for line in sql_lines if line and not line.strip().startswith("--")
    ]
    sql_to_parse = "\n".join(clean_sql_lines)

    # Preprocess to remove or replace invalid tokens
    sql_to_parse = sql_to_parse.replace("datetime", "")
    sql_to_parse = re.sub(r"\*", "", sql_to_parse)

    try:
        lineage_result = LineageRunner(sql_to_parse)
        if lineage_result:
            try:
                z = lineage_result.target_tables
                result_deps = [str(x) for x in z if "<default>." not in str(x)]
            except Exception as e:
                result_deps = []

            if len(result_deps) > 0:
                last_result = result_deps[-1]
                if "." in last_result:
                    database, table_name = last_result.split(".", 1)
                else:
                    table_name = last_result
    except SQLLineageException:
        pass

    if not table_name:
        found_tables = []
        f = code_to_parse.split("\n")
        for line in f:
            if (
                "merge into" in line.lower()
                or "insert into" in line.lower()
                or "insert overwrite table" in line.lower()
                or "insert overwrite" in line.lower()
                or "create or replace table" in line.lower()
                or "set vdp.output =" in line.lower()
                or "table_name = " in line.lower()
            ):
                if len(line.split(".")) > 1 and "spark." not in line:
                    line = (
                        line.lower()
                        .replace("merge into", "")
                        .replace("insert into", "")
                        .replace("insert overwrite table", "")
                        .replace("insert overwrite", "")
                        .replace("create or replace table", "")
                        .replace("set vdp.output =", "")
                        .replace("table_name =", "")
                    )
                    database, table_name = (
                        line.split(".")[0].strip(),
                        line.split(".")[1].split(" ")[0].strip(),
                    )
                    found_tables.append(f"{database}.{table_name}")

        if len(found_tables) > 0:
            found_token = found_tables[-1]

            found_token = [x for x in found_token.split(" ") if "." in x]

            if len(found_token) > 0:
                found_token = found_token[0]

            database = found_token.split(".")[0]
            table_name = found_token.split(".")[1]

    if (
        database
        and database.strip()
        and table_name
        and table_name.strip()
        and len(database.split(" ")) == 1
        and len(table_name.split(" ")) == 1
        and "{" not in database
    ):
        res = f"{database}.{table_name}".strip().replace('"', "").strip()
        db, tbl = res.split(".")
        db, tbl = db.strip(), tbl.strip()
        if db and tbl:
            return res
        else:
            return None
    else:
        return None


def get_recently_updated_tables(context) -> List[str]:
    queries = run_dbx_query(
        """SELECT
            request_params.commandText,
            event_time
        FROM
            system.access.audit
        WHERE
            event_date >= date_sub(current_date(), 2)
            AND request_params.commandText IS NOT NULL
            AND (
            response.status_code != 1
            OR response.status_code IS NULL
            )
            AND workspace_id = 5924096274798303 -- Add the workspace ID filter
            AND (
            LOWER(request_params.commandText) LIKE '%insert into %'
            or LOWER(request_params.commandText) LIKE '%merge into %'
            or LOWER(request_params.commandText) LIKE '%update %'
            or LOWER(request_params.commandText) LIKE '%delete from %'
            or LOWER(request_params.commandText) LIKE '%create table %'
            or LOWER(request_params.commandText) LIKE '%create or replace table %'
            or LOWER(request_params.commandText) LIKE '%insertinto(%'
            or LOWER(request_params.commandText) LIKE '%saveastable(%'
            or LOWER(request_params.commandText) LIKE '%save(%'
            )
               """
    )

    y = {x.commandText: x.event_time for x in queries}

    update_map = {}

    for q, ts in y.items():
        try:
            tbl = parse_table_from_submitted_query(q)
            if tbl and tbl not in update_map:
                update_map[tbl] = ts
            elif tbl and tbl in update_map:
                update_map[tbl] = max(update_map[tbl], ts)
        except (
            Exception
        ):  # this is fine to swallow since we can just skip anything that doesn't parse correctly
            pass

    last_query_update_map = pd.DataFrame(
        list(update_map.items()), columns=["table_name", "last_write_query_update"]
    )

    mrus = run_dbx_query(
        """with queries as (
                select
                    concat(database_name, '.', table_name) as table_name,
                    count(1) as num_queries
                from
                    auditlog.databricks_tables_queried
                where
                    date >= date_sub(current_date(), 30)
                group by
                    1
                ),
                all_tables as (
                select
                    table_schema as db,
                    table_name as tbl,
                    domain,
                    last_altered,
                    last_update_in_datahub
                from
                    system.information_schema.tables
                    join (
                    select
                        database,
                        table,
                        domain,
                        cast(datahub_scrapes.last_update / 1000 as timestamp) as last_update_in_datahub
                    from
                        auditlog.datahub_scrapes
                    where
                        date = (
                        select
                            max(date)
                        from
                            auditlog.datahub_scrapes
                        )
                    ) ds on ds.database = table_schema
                    and ds.table = table_name
                where
                    tables.table_type in ('MANAGED', 'EXTERNAL')
                    and data_source_format != 'CSV'
                    and domain != 'no_domain'
                ),
                t as (
                SELECT
                    concat(target_table_schema, '.', target_table_name) as table_name,
                    max(event_time) as table_last_updated
                from
                    system.access.table_lineage
                where
                    target_type = 'TABLE'
                group by
                    1
                ),
                t2 as (
                select
                concat(all_tables.db, '.', all_tables.tbl) as table_name,
                coalesce(queries.num_queries, 0) as num_queries,
                table_last_updated,
                last_altered,
                greatest(table_last_updated, last_altered) as last_update_time,
                last_update_in_datahub,
                domain,
                tc.production,
                tc.table_type -- scr.table_production_type,
                -- scr.file_location
                from
                all_tables
                left join dataplatform.table_classifications tc on concat(all_tables.db, '.', all_tables.tbl) = concat(tc.db, '.', tc.table)
                left join t on t.table_name = concat(all_tables.db, '.', all_tables.tbl)
                left join queries on queries.table_name = concat(all_tables.db, '.', all_tables.tbl) -- inner join datamodel_dev.dim_table_source_code_scrapes scr on scr.table = concat(all_tables.db, '.', all_tables.tbl)
                )

                select table_name, greatest(table_last_updated, last_altered, last_update_in_datahub) as last_observed_update_time from t2
                """
    )

    mru_df = pd.DataFrame(mrus, columns=["table_name", "last_observed_update_time"])

    mru_df = mru_df.merge(last_query_update_map, on="table_name", how="left")

    mru_df["last_observed_update_time"] = mru_df[
        ["last_observed_update_time", "last_write_query_update"]
    ].max(axis=1)

    mru_df = mru_df.drop(columns=["last_write_query_update"])

    # Filter for tables updated in the last 2 days
    two_days_ago = pd.Timestamp.now() - pd.Timedelta(days=2)
    mru_df = mru_df[
        pd.to_datetime(mru_df.last_observed_update_time).dt.tz_localize(None)
        >= two_days_ago
    ]

    context.log.info(f"num tables updated in last 2 days: {len(mru_df)}")

    mru_tables = list(mru_df.table_name)

    context.log.info(f"tables updated in last 2 days: {mru_tables}")

    return mru_tables
