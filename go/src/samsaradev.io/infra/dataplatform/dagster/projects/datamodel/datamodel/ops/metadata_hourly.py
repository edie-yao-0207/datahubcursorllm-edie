import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Union

import boto3
import datahub.emitter.mce_builder as builder
import pandas as pd
import pytz
from botocore.exceptions import ClientError
from dagster import In, Nothing, Out, asset, op
from databricks import sql
from datahub.api.entities.corpgroup.corpgroup import (
    CorpGroup,
    CorpGroupGenerationConfig,
)
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import create_embed_mcp
from datahub.emitter.rest_emitter import DataHubRestEmitter, DatahubRestEmitter
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
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
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    DatasetPropertiesClass,
    OperationClass,
    OperationTypeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from pyspark.sql import functions as F
from pyspark.sql.types import *

from ..common.datahub_utils import (
    database_filter,
    emit_event,
    emit_structured_properties,
    get_all_partitions_from_s3,
    get_all_urns,
    get_asset_specs_by_key,
    get_datahub_eks_s3_client,
    get_db_and_table,
    get_dbx_users,
    get_emitter,
    get_graph,
    get_latest_merge_or_insert,
    get_recently_updated_tables,
    get_repos,
    get_s3_objects,
    hash_object,
    log_op_duration,
    parse_table_from_submitted_query,
    run_dbx_query,
    run_graphql,
    scrape_datahub_op_name,
)
from ..common.utils import (
    AWSRegions,
    get_datahub_env,
    get_gms_server,
    get_run_env,
    initialize_datadog,
    send_datadog_gauge_metric,
)

os.environ["DATAHUB_TELEMETRY_ENABLED"] = "false"

if get_datahub_env() == "prod":
    METRIC_PREFIX = "datahub"
else:
    METRIC_PREFIX = "datahub.dev"

token = os.getenv("DATAHUB_GMS_TOKEN")
if os.getenv("MODE") == "LOCAL_DATAHUB_RUN":
    gms_server = "http://localhost:8080"
else:
    gms_server = get_gms_server()

EMIT_PERMISSIONS = False  # intentionally False

# Map DQ check operator strings to DataHub AssertionStdOperator
DQ_OPERATOR_MAP = {
    "eq": AssertionStdOperator.EQUAL_TO,
    "gte": AssertionStdOperator.GREATER_THAN_OR_EQUAL_TO,
    "gt": AssertionStdOperator.GREATER_THAN,
    "lte": AssertionStdOperator.LESS_THAN_OR_EQUAL_TO,
    "lt": AssertionStdOperator.LESS_THAN,
    "between": AssertionStdOperator.BETWEEN,
}


def date_string_to_timestamp_ms(
    date_string: str, offset_hours: int = 12, context: Any = None
) -> int:
    """Convert date string to timestamp in milliseconds.

    Args:
        date_string: Date string in format "YYYY-MM-DD" or with separators like
                     "YYYY-MM-DD|UNPARTITIONED_RUN" or "partition:YYYY-MM-DD"
        offset_hours: Hours to offset the timestamp (default 12 to ensure correct day)
        context: Optional context for logging warnings

    Returns:
        Timestamp in milliseconds
    """
    try:
        # Handle partition_str format like "2025-12-17|UNPARTITIONED_RUN" or "partition:2025-12-17"
        if "|" in date_string:
            date_string = date_string.split("|")[0]
        elif ":" in date_string:
            date_string = date_string.split(":")[1]

        # Convert the date string to a datetime object
        date_obj = datetime.strptime(date_string, "%Y-%m-%d")

        # Convert the datetime object to a Unix timestamp in seconds
        timestamp = time.mktime(date_obj.timetuple())

        # Convert the timestamp to milliseconds
        timestamp_ms = int(timestamp * 1000)

        # Apply offset
        return timestamp_ms + (offset_hours * 60 * 60 * 1000)
    except Exception as e:
        if context:
            context.log.warning(f"Failed to parse date '{date_string}': {e}")
        # Return current time if parsing fails
        return int(time.time() * 1000)


def extract_table_name_from_op_name(op_name: str, database: str) -> str:
    """Extract full table name from op_name by stripping 'dq_' prefix.

    Args:
        op_name: Operation name, potentially with 'dq_' prefix
        database: Database name

    Returns:
        Full table name in format "database.table"
    """
    if op_name.startswith("dq_"):
        table_name = op_name[3:]  # strip 'dq_' prefix
    else:
        table_name = op_name
    return f"{database}.{table_name}"


def create_assertion_info(
    check_name: str,
    full_table_name: str,
    operator: AssertionStdOperator,
    comparison_value: Union[str, int, float],
    native_type: str = "dq_check",
    native_parameters: Optional[Dict[str, Any]] = None,
    dataset_urn_fn: Optional[Callable[[str], str]] = None,
) -> AssertionInfo:
    """Create AssertionInfo for a DQ check.

    Args:
        check_name: Name of the DQ check
        full_table_name: Full table name in format "database.table"
        operator: AssertionStdOperator enum value
        comparison_value: Expected/comparison value
        native_type: Native type for the assertion (default: "dq_check")
        native_parameters: Optional native parameters dict
        dataset_urn_fn: Function to create dataset URN (default: uses builder.make_dataset_urn)

    Returns:
        AssertionInfo object
    """

    def _default_dataset_urn_fn(tbl: str) -> str:
        platform = "databricks"
        return builder.make_dataset_urn(platform, tbl)

    if dataset_urn_fn is None:
        dataset_urn_fn = _default_dataset_urn_fn

    result_type = AssertionStdParameterType.NUMBER
    if isinstance(comparison_value, str):
        result_type = AssertionStdParameterType.STRING

    if native_parameters is None:
        native_parameters = {"name": check_name}

    return AssertionInfo(
        type=AssertionType.DATASET,
        description=check_name,
        datasetAssertion=DatasetAssertionInfo(
            scope=DatasetAssertionScope.DATASET_ROWS,
            nativeType=native_type,
            nativeParameters=native_parameters,
            operator=operator,
            dataset=dataset_urn_fn(full_table_name),
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    type=result_type,
                    value=str(comparison_value),
                )
            ),
        ),
    )


def parse_observed_value_and_create_native_results(
    observed_value: Any, url_link: str, context: Optional[Any] = None
) -> Tuple[float, Dict[str, str]]:
    """Parse observed_value to float and create native_results dict.

    Args:
        observed_value: Observed value from DQ check (can be string, int, float, etc.)
        url_link: URL link to Dagster run
        context: Optional context for logging

    Returns:
        Tuple of (actual_result as float, native_results dict)
    """
    actual_result = 0
    try:
        actual_result = float(observed_value)
    except (ValueError, TypeError) as e:
        if context:
            context.log.info(f"Could not convert observed_value to float: {e}")
        pass

    native_results = {"result": str(observed_value), "run_url": url_link}

    return actual_result, native_results


def create_assertion_run_event(
    assertion_urn: str,
    assertee_urn: str,
    run_id: str,
    status: str,
    observed_value: Any,
    timestamp_ms: int,
    url_link: str,
    context: Optional[Any] = None,
) -> AssertionRunEvent:
    """Create AssertionRunEvent from DQ check result data.

    Args:
        assertion_urn: URN of the assertion
        assertee_urn: URN of the dataset being asserted
        run_id: Dagster run ID
        status: Status string ("success" or "failure")
        observed_value: Observed value from DQ check
        timestamp_ms: Timestamp in milliseconds
        url_link: URL link to Dagster run
        context: Optional context for logging

    Returns:
        AssertionRunEvent object
    """
    result_type = (
        AssertionResultType.SUCCESS
        if status == "success"
        else AssertionResultType.FAILURE
    )

    actual_result, native_results = parse_observed_value_and_create_native_results(
        observed_value, url_link, context
    )

    return AssertionRunEvent(
        timestampMillis=int(timestamp_ms),
        assertionUrn=assertion_urn,
        asserteeUrn=assertee_urn,
        runId=run_id,
        status=AssertionRunStatus.COMPLETE,
        result=AssertionResult(
            type=result_type,
            actualAggValue=actual_result,
            nativeResults=native_results,
            externalUrl=url_link,
        ),
    )


initialize_datadog()


db_shard_map = {
    "biztech_edw_dataquality_gold": 2,
    "biztech_sdas": 2,
    "dojo": 2,
    "labelbox": 2,
    "growth": 2,
    "data_analytics": 2,
    "datastreams": 2,
    "datastreams_history": 2,
    "datastreams_schema": 2,
    "datastreams_view_test": 2,
    "firmware_dev": 2,
    "ksdifftool": 2,
    "people_analytics": 2,
    "platops": 2,
    "hardware_analytics": 2,
    "kinesisstats": 0,
    "productsdb": 0,
    "clouddb": 0,
    "datamodel_core": 0,
    "datamodel_telematics": 0,
    "datamodel_safety": 0,
    "datamodel_platform": 0,
    "datamodel_core_silver": 0,
    "datamodel_telematics_silver": 0,
    "datamodel_safety_silver": 0,
    "datamodel_platform_silver": 0,
    "datamodel_core_bronze": 0,
    "datamodel_telematics_bronze": 0,
    "datamodel_safety_bronze": 0,
    "datamodel_platform_bronze": 0,
    "mixpanel_samsara": 0,
    "customer360": 0,
    "product_analytics": 0,
    "product_analytics_staging": 0,
    "dataengineering": 0,
    "feature_store": 0,
    "inference_store": 0,
    "material_usage_report": 0,
    "cm_health_report": 0,
    "dataprep": 0,
    "dataprep_firmware": 0,
    "dataprep_safety": 0,
    "dataprep_telematics": 0,
}


def get_hourly_op_for_shard(shard):
    @op(
        name=f"extract_metadata_to_datahub_hourly_{shard}",
        ins={"start": In(Nothing)},
        out=Out(io_manager_key="in_memory_io_manager"),
        required_resource_keys=None,
        retry_policy=None,
    )
    def extract_hourly_op(context) -> int:

        op_start_time = int(time.time())

        graph = get_graph(context)
        urns = get_all_urns(context)

        # for tables still likely to OOM, we should run at the end so the rest of tables get their updates
        last_tables = [
            make_dataset_urn("databricks", table)
            for table in ["dataprep.osdaccelerometer_event_traces"]
        ]
        last_urns = []

        urns_sorted = []

        # to be conservative we will check any table that was updated via any signal we have in the last 3 days
        # current signals: UC lineage events, UC table update timestamps, lagged DataHub S3 scrapes
        mru_tables = get_recently_updated_tables(context)

        for urn in urns:
            current_hour = datetime.now(timezone.utc).hour
            catalog, database, table = get_db_and_table(urn["entity"]["urn"])

            if (
                f"{database}.{table}" not in mru_tables and current_hour != 12
            ):  # update all tables at 12hr mark
                continue

            if db_shard_map.get(database, 1) != shard:
                continue

            if urn["entity"]["urn"] not in last_tables:
                urns_sorted.append(urn)
            else:
                last_urns.append(urn)

        for urn in last_urns:
            if db_shard_map.get(database, 0) != shard:
                continue
            urns_sorted.append(urn)

        context.log.info(f"processing {len(urns_sorted)} tables")

        cur_time = int(time.time())
        start_time = cur_time

        i = 0
        last_urn = None

        eu_table_s3_map = run_dbx_query(
            """
                    select concat(table_schema, '.', table_name) as table_name,
                    storage_path from system.information_schema.tables
            """,
            region="eu-west-1",
        )

        eu_table_s3_map = {row.table_name: row.storage_path for row in eu_table_s3_map}

        ca_table_s3_map = run_dbx_query(
            """
                    select concat(table_schema, '.', table_name) as table_name,
                    storage_path from system.information_schema.tables
            """,
            region="ca-central-1",
        )

        ca_table_s3_map = {row.table_name: row.storage_path for row in ca_table_s3_map}

        for urn in urns_sorted:
            s3_location = ""
            i += 1
            last_time = cur_time
            cur_time = int(time.time())
            elapsed = cur_time - last_time
            total_elapsed = cur_time - start_time
            if elapsed >= 15:
                context.log.warning(
                    f"elapsed: {str(round(elapsed,2))} seconds for {last_urn}"
                )

            if i % 100 == 0:
                context.log.info(f"processed {i} of {len(urns_sorted)} tables")
                context.log.info(
                    f"average time per table: {round(total_elapsed/i,2)} seconds per table"
                )
                context.log.debug(
                    f"estimated total run time: {round((total_elapsed/i)*len(urns_sorted)/60,2)} minutes"
                )

            urn = urn["entity"]["urn"]
            last_urn = urn

            current_properties = graph.get_aspect(
                entity_urn=urn, aspect_type=DatasetPropertiesClass
            )

            if hasattr(current_properties, "customProperties"):
                s3_location = current_properties.customProperties.get(
                    "location",
                    current_properties.customProperties.get("storage_location"),
                )
            else:
                context.log.info(f"no customProperties for {urn}")

            if not s3_location or not s3_location.startswith("s3://"):
                pass
            else:
                bucket_name = s3_location.replace("s3://", "").split("/")[0]
                delta_table_prefix = (
                    s3_location.replace("s3://", "").replace(bucket_name, "").strip("/")
                )

                if os.getenv("MODE") in (
                    "LOCAL_PROD_CLUSTER_RUN",
                    "DRY_RUN",
                    "LOCAL_DATAHUB_RUN",
                ):
                    s3 = boto3.client("s3")
                else:
                    s3 = get_datahub_eks_s3_client()

                last_update_us = get_latest_merge_or_insert(
                    context,
                    s3,
                    bucket_name,
                    delta_table_prefix,
                    value_to_find="last_update",
                )

                if not last_update_us:
                    continue

                last_update_from_graph = graph.get_latest_timeseries_value(
                    entity_urn=urn,
                    aspect_type=OperationClass,
                    filter_criteria_map={"operationType": OperationTypeClass.UPDATE},
                )

                sts_client = boto3.client("sts")
                assumed_role_object = sts_client.assume_role(
                    RoleArn="arn:aws:iam::492164655156:role/datahub-eks-nodes",
                    RoleSessionName="datahub-eks-nodes-session",
                )
                credentials = assumed_role_object["Credentials"]

                # Use the assumed credentials
                session = boto3.Session(
                    aws_access_key_id=credentials["AccessKeyId"],
                    aws_secret_access_key=credentials["SecretAccessKey"],
                    aws_session_token=credentials["SessionToken"],
                )
                s3 = session.client("s3")

                all_partitions_in_s3 = {}

                # initially only roll out partition logging for datamodel tables
                catalog, database, table = get_db_and_table(urn)
                if database.startswith("datamodel_") or database in [
                    "auditlog",
                    "product_analytics",
                    "product_analytics_staging",
                    "dataengineering",
                    "feature_store",
                    "inference_store",
                ]:
                    # find all partitions for updated tables
                    all_partitions_in_s3[
                        AWSRegions.US_WEST_2.value
                    ] = get_all_partitions_from_s3(
                        context,
                        s3,
                        bucket_name,
                        delta_table_prefix,
                    )

                    if all_partitions_in_s3.get(AWSRegions.US_WEST_2.value):
                        min_date_us = min(
                            all_partitions_in_s3[AWSRegions.US_WEST_2.value]
                        )
                        max_date_us = max(
                            all_partitions_in_s3[AWSRegions.US_WEST_2.value]
                        )

                    try:
                        eu_bucket_name = eu_table_s3_map.get(f"{database}.{table}")

                        if not eu_bucket_name:
                            eu_bucket_name = bucket_name.replace(
                                "samsara-", "samsara-eu-"
                            )
                        else:
                            full_path = eu_bucket_name.split("s3://")[1]
                            eu_bucket_name = full_path.split("/")[0]
                            delta_table_prefix = "/".join(full_path.split("/")[1:])

                        last_update_eu = get_latest_merge_or_insert(
                            context,
                            s3,
                            eu_bucket_name,
                            delta_table_prefix,
                            value_to_find="last_update",
                        )

                        all_partitions_in_s3[
                            AWSRegions.EU_WEST_1.value
                        ] = get_all_partitions_from_s3(
                            context,
                            s3,
                            eu_bucket_name,
                            delta_table_prefix,
                        )

                        if all_partitions_in_s3.get(AWSRegions.EU_WEST_1.value):
                            min_date_eu = min(
                                all_partitions_in_s3[AWSRegions.EU_WEST_1.value]
                            )
                            max_date_eu = max(
                                all_partitions_in_s3[AWSRegions.EU_WEST_1.value]
                            )

                    except Exception as e:
                        context.log.info(f"error for {urn}: {e}")
                        pass

                    try:
                        ca_bucket_name = ca_table_s3_map.get(f"{database}.{table}")

                        if not ca_bucket_name:
                            ca_bucket_name = bucket_name.replace(
                                "samsara-", "samsara-ca-"
                            )
                        else:
                            full_path = ca_bucket_name.split("s3://")[1]
                            ca_bucket_name = full_path.split("/")[0]
                            delta_table_prefix = "/".join(full_path.split("/")[1:])

                        last_update_ca = get_latest_merge_or_insert(
                            context,
                            s3,
                            ca_bucket_name,
                            delta_table_prefix,
                            value_to_find="last_update",
                        )

                        all_partitions_in_s3[
                            AWSRegions.CA_CENTRAL_1.value
                        ] = get_all_partitions_from_s3(
                            context,
                            s3,
                            ca_bucket_name,
                            delta_table_prefix,
                        )

                        if all_partitions_in_s3.get(AWSRegions.CA_CENTRAL_1.value):
                            min_date_ca = min(
                                all_partitions_in_s3[AWSRegions.CA_CENTRAL_1.value]
                            )
                            max_date_ca = max(
                                all_partitions_in_s3[AWSRegions.CA_CENTRAL_1.value]
                            )

                    except Exception as e:
                        context.log.info(f"error for {urn}: {e}")
                        pass

                    # write available date ranges to DataHub

                    structured_properties = {}

                    if all_partitions_in_s3.get(AWSRegions.US_WEST_2.value):
                        structured_properties["min_date_us"] = min_date_us
                        structured_properties["max_date_us"] = max_date_us

                    if all_partitions_in_s3.get(AWSRegions.EU_WEST_1.value):
                        structured_properties["min_date_eu"] = min_date_eu
                        structured_properties["max_date_eu"] = max_date_eu

                    if all_partitions_in_s3.get(AWSRegions.CA_CENTRAL_1.value):
                        structured_properties["min_date_ca"] = min_date_ca
                        structured_properties["max_date_ca"] = max_date_ca

                    emit_structured_properties(graph, urn, structured_properties)

                    # log landed partitions to S3

                    s3_client = boto3.client("s3")

                    regions_to_log = [AWSRegions.US_WEST_2.value]

                    if all_partitions_in_s3.get(AWSRegions.EU_WEST_1.value) or (
                        last_update_eu or 0
                    ) >= (int(time.time()) - 24 * 60 * 60 * 1000):
                        regions_to_log.append(AWSRegions.EU_WEST_1.value)

                    if all_partitions_in_s3.get(AWSRegions.CA_CENTRAL_1.value) or (
                        last_update_ca or 0
                    ) >= (int(time.time()) - 24 * 60 * 60 * 1000):
                        regions_to_log.append(AWSRegions.CA_CENTRAL_1.value)

                    for region in regions_to_log:
                        last_update = (
                            last_update_eu
                            if region == AWSRegions.EU_WEST_1.value
                            else (
                                last_update_ca
                                if region == AWSRegions.CA_CENTRAL_1.value
                                else last_update_us
                            )
                        )
                        current_partitions_logged_in_s3 = {}

                        key = f"staging/landed_partitions/{get_datahub_env()}/{region}/{database}/{table}.json"

                        try:
                            obj = s3_client.get_object(
                                Bucket="samsara-amundsen-metadata",
                                Key=key,
                            )

                            data = obj["Body"].read().decode("utf-8")

                            current_partitions_logged_in_s3 = json.loads(data)

                            if type(current_partitions_logged_in_s3) is list:
                                current_partitions_logged_in_s3 = {}

                        except ClientError as e:
                            if e.response["Error"]["Code"] == "NoSuchKey":
                                current_partitions_logged_in_s3 = {}

                        current_date_partition_str = datetime.now(
                            timezone.utc
                        ).strftime("%Y-%m-%d")
                        # only add if not already in partition list
                        if not all_partitions_in_s3[region]:
                            if (
                                current_date_partition_str
                                not in current_partitions_logged_in_s3
                            ):
                                current_partitions_logged_in_s3[
                                    current_date_partition_str
                                ] = (last_update / 1000)
                        else:
                            for partition in all_partitions_in_s3[region]:
                                if partition not in current_partitions_logged_in_s3:
                                    current_partitions_logged_in_s3[
                                        partition
                                    ] = all_partitions_in_s3[region][partition]

                        raw_json = json.dumps(current_partitions_logged_in_s3)

                        s3_client.put_object(
                            Body=raw_json,
                            Bucket="samsara-amundsen-metadata",
                            Key=key,
                        )

                if (
                    last_update_from_graph
                    and last_update_from_graph.timestampMillis >= last_update_us
                ):
                    continue

                emitter = get_emitter()

                operation_aspect = OperationClass(
                    timestampMillis=int(time.time() * 1000),
                    lastUpdatedTimestamp=last_update_us,
                    operationType=OperationTypeClass.UPDATE,
                )

                ev = MetadataChangeProposalWrapper(
                    entityUrn=urn, aspect=operation_aspect
                )

                emit_event(emitter, ev, extra_tags={"op": "hourly"})

        op_duration = int(time.time()) - op_start_time

        send_datadog_gauge_metric(
            metric=f"dagster.{METRIC_PREFIX}.job.run.duration",
            value=op_duration,
            tags=[f"op:{scrape_datahub_op_name()}"],
        )

        return 0

    return extract_hourly_op


extract_metadata_to_datahub_hourly_0 = get_hourly_op_for_shard(0)
extract_metadata_to_datahub_hourly_1 = get_hourly_op_for_shard(1)
extract_metadata_to_datahub_hourly_2 = get_hourly_op_for_shard(2)


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def extract_dq_checks_to_datahub(context) -> int:
    asset_specs_by_key = get_asset_specs_by_key()
    emitter = get_emitter()

    # emit DQ checks - will later refactor these into utils
    def emitAssertionResult(assertionResult: AssertionRunEvent) -> None:
        dataset_assertionRunEvent_mcp = MetadataChangeProposalWrapper(
            entityUrn=assertionResult.assertionUrn,
            aspect=assertionResult,
        )

        emit_event(emitter, dataset_assertionRunEvent_mcp, event_type="mcp")

    def datasetUrn(tbl: str) -> str:
        platform = "databricks"
        return builder.make_dataset_urn(platform, tbl)

    def assertionUrn(dq_check: dict, full_table_name: str) -> str:
        dq_check["full_table_name"] = full_table_name
        return f"urn:li:assertion:{hash_object(dq_check)}"

    # find dq check by querying auditlog.dq_check_log where database = 'inference_store' and add those
    def _emit_dq_checks_from_auditlog():
        db = "auditlog"
        lookback_days = 7

        # at first we should only use this for inference_store.device_known_locations
        # to avoid duplicative processing. Can generalize later.
        query = f"""
        WITH t AS (
            SELECT
                op_name,
                database,
                partition_str,
                dagster_run_id,
                `key` AS check_name,
                CASE
                    WHEN LOCATE('|', partition_str) > 0 THEN CAST(split(partition_str, '\\\|')[0] AS STRING)
                    WHEN LOCATE(':', partition_str) > 0 THEN CAST(split(partition_str, '\\\:')[1] AS STRING)
                    ELSE partition_str
                END as parsed_date,
                value['Status'] AS status,
                value['Operator'] AS operator,
                value['Comparison Value'] AS comparison_value,
                value['Observed Value'] AS observed_value
            FROM
                (
                    SELECT
                        op_name,
                        database,
                        partition_str,
                        dagster_run_id,
                        EXPLODE(results) AS (`key`, value)
                    FROM
                        default.{db}.dq_check_log
                    WHERE
                        database = 'inference_store'
                        AND op_name = 'dq_device_known_locations'
                    ORDER BY
                        partition_str DESC
                )
        ),
        RankedData AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY check_name,
                    database,
                    op_name,
                    parsed_date
                    ORDER BY
                        dagster_run_id DESC,
                        partition_str DESC
                ) as rn
            FROM
                t
            WHERE
                parsed_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
        )
        SELECT
            op_name,
            database,
            partition_str,
            dagster_run_id,
            check_name,
            status,
            operator,
            comparison_value,
            observed_value,
            parsed_date
        FROM
            RankedData
        WHERE
            rn = 1
        """
        result = run_dbx_query(query)
        context.log.info(
            f"Found {len(result)} DQ check results from {db}.dq_check_log "
            f"(last {lookback_days} days, deduplicated)"
        )

        if not result:
            return result

        # Group results by (op_name, check_name) to create unique assertions
        assertion_map = {}  # (op_name, check_name) -> assertion info

        # First pass: create assertions for each unique (op_name, check_name) combination
        for row in result:
            key = (row.op_name, row.check_name)
            if key in assertion_map:
                continue

            # Extract table name from op_name (strip 'dq_' prefix if present)
            op_name = row.op_name
            full_table_name = extract_table_name_from_op_name(op_name, row.database)

            # Map operator
            operator_str = row.operator.lower() if row.operator else "eq"
            operator = DQ_OPERATOR_MAP.get(operator_str, AssertionStdOperator.EQUAL_TO)

            # Create assertion info
            dq_check_dict = {
                "name": row.check_name,
                "full_table_name": full_table_name,
                "operator": operator_str,
                "comparison_value": str(row.comparison_value),
            }

            assertion = create_assertion_info(
                check_name=row.check_name,
                full_table_name=full_table_name,
                operator=operator,
                comparison_value=row.comparison_value,
                native_parameters={
                    "name": row.check_name,
                    "op_name": op_name,
                    "database": row.database,
                },
                dataset_urn_fn=datasetUrn,
            )

            assertion_urn = assertionUrn(dq_check_dict, full_table_name)
            assertion_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertion_urn,
                aspect=assertion,
            )

            emit_event(emitter, assertion_mcp, event_type="mcp")
            context.log.info(
                f"Emitted assertion for {full_table_name}: {row.check_name}"
            )

            assertion_map[key] = {
                "assertion_urn": assertion_urn,
                "full_table_name": full_table_name,
                "operator": operator,
            }

        # Second pass: emit assertion results for each row
        for row in result:
            key = (row.op_name, row.check_name)
            assertion_info = assertion_map.get(key)
            if not assertion_info:
                continue

            # Use parsed_date directly from query result
            parsed_date = row.parsed_date
            timestamp_ms = date_string_to_timestamp_ms(parsed_date, context=context)

            url_link = f"https://dagster.internal.samsara.com/runs/{row.dagster_run_id}"

            assertion_result = create_assertion_run_event(
                assertion_urn=assertion_info["assertion_urn"],
                assertee_urn=datasetUrn(assertion_info["full_table_name"]),
                run_id=row.dagster_run_id,
                status=row.status,
                observed_value=row.observed_value,
                timestamp_ms=timestamp_ms,
                url_link=url_link,
                context=context,
            )

            emitAssertionResult(assertion_result)
            context.log.info(
                f"Emitted assertion result for {row.check_name} on {parsed_date}: {row.status}"
            )

        context.log.info(
            f"Successfully emitted {len(assertion_map)} assertions and {len(result)} assertion results"
        )

        return result

    _emit_dq_checks_from_auditlog()

    def _emit_dq_checks_from_assets():
        dq_check_map = {}

        def _get_dq_data(context, op_name, lookback_days=7) -> None:

            query = f"""
            with t as (
                SELECT
                    op_name,
                    database,
                    partition_str,
                    dagster_run_id,
                    `key` AS check_name,
                    CASE
                    WHEN LOCATE('|', partition_str) > 0 THEN CAST(split(partition_str, '\\\|')[0] AS STRING)
                    WHEN LOCATE(':', partition_str) > 0 THEN CAST(split(partition_str, '\\\:')[1] AS STRING)
                    ELSE partition_str
                    END as parsed_date,
                    value ['Status'] AS status,
                    value ['Operator'] AS operator,
                    value ['Comparison Value'] AS comparison_value,
                    value ['Observed Value'] AS observed_value
                FROM
                    (
                    SELECT
                        op_name,
                        database,
                        partition_str,
                        dagster_run_id,
                        EXPLODE(results) AS (`key`, value)
                    FROM
                        default.auditlog.dq_check_log
                    WHERE
                        op_name = '{op_name}'
                    ORDER BY
                        partition_str DESC
                    )
                ),
            RankedData AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                PARTITION BY check_name,
                database,
                op_name,
                parsed_date
                ORDER BY
                    status ASC
                ) as rn
            FROM
                t
                where parsed_date >= DATE_SUB(CURRENT_DATE(), {lookback_days})
            )
            SELECT
            *
            FROM
            RankedData
            WHERE
            rn = 1
            """

            result = run_dbx_query(query)

            context.log.info(result)

            return result

        for asset_key, asset_spec in asset_specs_by_key.items():
            if len(asset_key.path) != 3:
                continue
            region, database, table = tuple(asset_key.path)
            dq_check_name = table

            if region == AWSRegions.US_WEST_2.value:
                full_table_name = extract_table_name_from_op_name(table, database)
                full_table_name = full_table_name.split("__")[
                    0
                ]  # should actually do this for all tables

                dq_checks = asset_spec.metadata.get("dq_checks", [])

                if not dq_checks:
                    continue

                if not dq_check_name.startswith("dq_"):
                    dq_check_name = f"dq_{dq_check_name}"

                for dq_check in dq_checks:

                    operator = AssertionStdOperator.EQUAL_TO

                    op = dq_check["details"].get("operator", "eq")

                    expected_value = dq_check["details"].get("expected_value", 0)

                    if op != "eq":
                        operator = DQ_OPERATOR_MAP.get(
                            op, AssertionStdOperator.EQUAL_TO
                        )

                    assertion = create_assertion_info(
                        check_name=dq_check["name"],
                        full_table_name=full_table_name,
                        operator=operator,
                        comparison_value=expected_value,
                        native_type=dq_check["type"],
                        native_parameters={
                            "name": dq_check["name"],
                            "blocking": str(dq_check["blocking"]),
                            "details": str(dq_check["details"]),
                        },
                        dataset_urn_fn=datasetUrn,
                    )

                    assertion_mcp = MetadataChangeProposalWrapper(
                        entityUrn=assertionUrn(dq_check, full_table_name),
                        aspect=assertion,
                    )

                    emit_event(emitter, assertion_mcp, event_type="mcp")

                    if dq_check["name"] in dq_check_map:
                        raise ValueError(f"duplicate dq check name: {dq_check['name']}")

                    dq_check_map[dq_check["name"]] = dq_check

                DQ_LOOKBACK_DAYS = 7

                most_recent_results = _get_dq_data(
                    context, dq_check_name, lookback_days=DQ_LOOKBACK_DAYS
                )

                if len(most_recent_results) > DQ_LOOKBACK_DAYS:
                    context.log.warning(
                        f"too many results for {dq_check_name} - multi-partitioned?"
                    )

                for row in most_recent_results:
                    dq_check = dq_check_map.get(row.check_name)
                    if not dq_check:
                        continue

                    date = row.parsed_date
                    timestamp_ms = date_string_to_timestamp_ms(date, context=context)

                    url_link = f"https://dagster.internal.samsara.com/runs/{row.dagster_run_id}"

                    assertion_result = create_assertion_run_event(
                        assertion_urn=assertionUrn(dq_check, full_table_name),
                        assertee_urn=datasetUrn(full_table_name),
                        run_id=row.dagster_run_id,
                        status=row.status,
                        observed_value=row.observed_value,
                        timestamp_ms=timestamp_ms,
                        url_link=url_link,
                        context=context,
                    )

                    emitAssertionResult(assertion_result)

    _emit_dq_checks_from_assets()

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_users_and_groups_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)
    urns = get_all_urns(context)

    users_in_graph = [
        x["entity"]["urn"] for x in get_all_urns(context, entity_type="CORP_USER")
    ]

    # create databricks users group
    graphql_query = """
                mutation createGroup($input: CreateGroupInput!) {
                    createGroup(input: $input)
                    }
                """

    result, err = run_graphql(
        graph,
        graphql_query,
        variables={
            "urn": "urn:li:corpGroup:databricks_users",
            "input": {
                "id": "databricks_users",
                "description": "Users with a Databricks account",
                "name": "DatabricksUsers",
            },
        },
    )

    if err:
        context.log.error(err)

    current_users_in_dbx_group, err = run_graphql(
        graph,
        graphql_query="""
                                      query getAllGroupMembers($urn: String!, $start: Int!, $count: Int!) {
                                        corpGroup(urn: $urn) {
                                            relationships(
                                            input: {types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING, start: $start, count: $count}
                                            ) {
                                            relationships {
                                                entity {
                                                ... on CorpUser {
                                                    urn
                                                }
                                                }
                                            }
                                            }
                                        }
                                        }""",
        variables={
            "urn": "urn:li:corpGroup:databricks_users",
            "start": 0,
            "count": 10000,
        },
    )

    current_users_in_dbx_group = [
        x["entity"]["urn"]
        for x in current_users_in_dbx_group["corpGroup"]["relationships"][
            "relationships"
        ]
    ]

    # retry with exponential backoff
    for i in range(5):
        raw_dbx_users, dbx_err = get_dbx_users()
        if raw_dbx_users:
            break
        elif dbx_err:
            context.log.error(dbx_err)
            backoff_seconds = 5**i + 10
            context.log.info(f"retrying in {backoff_seconds} seconds")
            time.sleep(backoff_seconds)

    context.log.info(f"retrieved ({len(raw_dbx_users)}) databricks users")

    if raw_dbx_users:
        dbx_users = [x["emails"] for x in raw_dbx_users]

        dbx_user_emails = [
            elem.get("value")
            for row in dbx_users
            for elem in row
            if (
                elem.get("type") == "work"
                and ("urn:li:corpuser:" + elem.get("value")) in users_in_graph
            )
        ]

        dbx_user_urns = ["urn:li:corpuser:" + y for y in dbx_user_emails]

        graphql_query = """
                    mutation addGroupMembers($input: AddGroupMembersInput!) {
                        addGroupMembers(input: $input)
                        }
                """

        group_urn = "urn:li:corpGroup:databricks_users"

        variables = {"input": {"groupUrn": group_urn, "userUrns": dbx_user_urns}}

        result, err = run_graphql(graph, graphql_query, variables=variables)
        if err:
            context.log.error(err)
    else:
        context.log.error("no dbx users found when querying dbx api")

    offboarded_dbx_users = [
        x for x in current_users_in_dbx_group if x not in dbx_user_urns
    ]

    if dbx_user_urns and offboarded_dbx_users:
        # remove offboarded members from databricks users group
        graphql_query = """
                    mutation removeGroupMembers($groupUrn: String!, $userUrns: [String!]!) {
                        removeGroupMembers(input: {groupUrn: $groupUrn, userUrns: $userUrns})
                        }
                """

        variables = {
            "groupUrn": "urn:li:corpGroup:databricks_users",
            "userUrns": offboarded_dbx_users,
        }

        result, err = run_graphql(
            graph, graphql_query=graphql_query, variables=variables
        )
        if err:
            context.log.error(err)

    table_owners_from_s3_records = get_s3_objects("table_owners")

    teams_from_s3_records = get_s3_objects("teams")

    # emit groups
    def _process_team(team):
        team_id = team["TeamName"].lower()

        urn = f"urn:li:corpGroup:{team_id}"

        subteams = team.get("SubTeams", [])

        team_slack_channel = team.get(
            "SlackContactChannel", team.get("SlackAlertsChannel", None)
        )

        if subteams:
            for subteam in subteams:
                _process_team(subteam)

        members = {x["Name"]: x["Email"] for x in (team["Members"] or [])}
        owners = {
            x["Name"]: x["Email"] for x in (team["Members"] or []) if x["Manager"]
        }

        description = (
            team.get("Description", "")
            + "\n\n"
            + f"[Backstage URL](https://backstage.internal.samsara.com/catalog/default/group/{team_id})"
        )

        group_from_graph = graph.get_aspect(
            entity_urn=urn, aspect_type=CorpGroupInfoClass
        )

        if not group_from_graph:
            graphql_query = """
                mutation createGroup($input: CreateGroupInput!) {
                    createGroup(input: $input)
                    }
                """

            result, err = run_graphql(
                graph,
                graphql_query,
                variables={
                    "urn": f"urn:li:corpGroup:{team_id}",
                    "input": {
                        "id": team_id,
                        "description": description,
                        "name": team["TeamName"],
                    },
                },
            )

            if err:
                context.log.error(err)

            # add slack token
            if team_slack_channel:
                graphql_query = """
                    mutation updateCorpGroupProperties($urn: String!, $input: CorpGroupUpdateInput!) {
                                updateCorpGroupProperties(urn: $urn, input: $input) {
                                    urn
                                }
                            }
                    """
                result, err = run_graphql(
                    graph,
                    graphql_query,
                    variables={
                        "urn": f"urn:li:corpGroup:{team_id}",
                        "input": {
                            "description": description,
                            "slack": team_slack_channel,
                        },
                    },
                )

                if err:
                    context.log.error(err)
        else:
            graphql_query = """
                mutation updateCorpGroupProperties($urn: String!, $input: CorpGroupUpdateInput!) {
                            updateCorpGroupProperties(urn: $urn, input: $input) {
                                urn
                            }
                        }
                """
            variables = {
                "urn": f"urn:li:corpGroup:{team_id}",
                "input": {
                    "description": description,
                    "slack": team_slack_channel,
                },
            }

            result, err = run_graphql(graph, graphql_query, variables)
            if err:
                context.log.error(err)

        members_in_graph = group_from_graph.members if group_from_graph else []

        ownership_aspect = graph.get_aspect(entity_urn=urn, aspect_type=OwnershipClass)

        if hasattr(ownership_aspect, "owners") and group_from_graph:
            owners_in_graph = ownership_aspect.owners
        else:
            owners_in_graph = []

        members_in_graph, err = run_graphql(
            graph,
            """
                    query getAllGroupMembers($urn: String!, $start: Int!, $count: Int!) {
                    corpGroup(urn: $urn) {
                        relationships(
                        input: {types: ["IsMemberOfGroup", "IsMemberOfNativeGroup"], direction: INCOMING, start: $start, count: $count}
                        ) {
                        relationships {
                            entity {
                            ... on CorpUser {
                                urn
                                username
                                editableProperties {
                                displayName
                                }
                            }
                            }
                        }
                        }
                    }
                    }
""",
            variables={"urn": urn, "start": 0, "count": 1000},
        )

        if err:
            context.log.error(err)

        members_in_graph = [
            x["entity"]["urn"]
            for x in members_in_graph["corpGroup"]["relationships"]["relationships"]
        ]

        new_members = {x: y for x, y in members.items() if y not in members_in_graph}
        new_member_urns = ["urn:li:corpuser:" + y for y in new_members.values()]
        offboarded_members = [
            x
            for x in members_in_graph
            if x not in ["urn:li:corpuser:" + y for y in members.values()]
        ]

        offboarded_managers = [
            x
            for x in owners_in_graph
            if x not in ["urn:li:corpuser:" + y for y in owners.values()]
        ]

        # remove offboarded members
        if offboarded_members:
            graphql_query = """
                mutation removeGroupMembers($groupUrn: String!, $userUrns: [String!]!) {
                    removeGroupMembers(input: {groupUrn: $groupUrn, userUrns: $userUrns})
                    }
            """

            variables = {"groupUrn": urn, "userUrns": offboarded_members}

            result, err = run_graphql(
                graph, graphql_query=graphql_query, variables=variables
            )
            if err:
                context.log.error(err)

        # remove all technical owners (since we use business owners)
        REMOVE_TECHNICAL_OWNERS = True

        if REMOVE_TECHNICAL_OWNERS:
            for manager in owners_in_graph:
                graphql_query = """
                        mutation removeOwner($input: RemoveOwnerInput!) {
                            removeOwner(input: $input)
                            }
                    """

                variables = {
                    "input": {
                        "ownerUrn": manager.owner,
                        "ownershipTypeUrn": "urn:li:ownershipType:__system__technical_owner",
                        "resourceUrn": f"urn:li:corpGroup:{team_id}",
                    }
                }

                result, err = run_graphql(graph, graphql_query, variables=variables)
                if err:
                    context.log.error(err)

        # remove offboarded managers
        if offboarded_managers:
            for offboarded_manager in offboarded_managers:
                graphql_query = """
                    mutation removeOwner($input: RemoveOwnerInput!) {
                        removeOwner(input: $input)
                        }
                """

                variables = {
                    "input": {
                        "ownerUrn": offboarded_manager.owner,
                        "ownershipTypeUrn": "urn:li:ownershipType:__system__business_owner",
                        "resourceUrn": f"urn:li:corpGroup:{team_id}",
                    }
                }

                result, err = run_graphql(graph, graphql_query, variables=variables)
                if err:
                    context.log.error(err)

        REFRESH_ALL_USER_NAMES = False

        # refresh all partitions each saturday
        if datetime.now().weekday() == 5 and datetime.now().hour == 6:
            context.log.info("refreshing all user names on Saturday at 6am")
            REFRESH_ALL_USER_NAMES = True

        if REFRESH_ALL_USER_NAMES:
            members_to_update = members
        else:
            members_to_update = new_members

        if members_to_update:
            for name, email in members_to_update.items():
                graphql_query = """
                        mutation updateCorpUserProperties($urn: String!, $input: CorpUserUpdateInput!) {
                        updateCorpUserProperties(urn: $urn, input: $input) {
                            urn
                        }
                        }
                """

                variables = {
                    "urn": f"urn:li:corpuser:{email}",
                    "input": {"displayName": name, "email": email},
                }
                result, err = run_graphql(graph, graphql_query, variables=variables)
                if err:
                    context.log.error(err)

            graphql_query = """
                mutation addGroupMembers($input: AddGroupMembersInput!) {
                    addGroupMembers(input: $input)
                    }
            """

            variables = {"input": {"groupUrn": urn, "userUrns": new_member_urns}}

            result, err = run_graphql(graph, graphql_query, variables=variables)
            if err:
                context.log.error(err)

        if owners:
            graphql_query = """
                    mutation batchAddOwners($input: BatchAddOwnersInput!) {
                        batchAddOwners(input: $input)
                        }
            """

            variables = {
                "input": {
                    "owners": [
                        {
                            "ownerUrn": "urn:li:corpuser:" + x,
                            "ownerEntityType": "CORP_USER",
                            "ownershipTypeUrn": "urn:li:ownershipType:__system__business_owner",
                        }
                        for x in owners.values()
                    ],
                    "resources": [{"resourceUrn": f"urn:li:corpGroup:{team_id}"}],
                }
            }

            result, err = run_graphql(graph, graphql_query, variables=variables)
            if err:
                context.log.error(err)

            graphql_query = """
                mutation addGroupMembers($input: AddGroupMembersInput!) {
                    addGroupMembers(input: $input)
                    }
            """

            variables = {"input": {"groupUrn": urn, "userUrns": new_member_urns}}

            result, err = run_graphql(graph, graphql_query, variables=variables)
            if err:
                context.log.error(err)

    for team in teams_from_s3_records:
        _process_team(team)

    amundsen_owner_map = dict(table_owners_from_s3_records)

    groups_from_amundsen = list(set((amundsen_owner_map.values())))

    groups_from_amundsen = [x for x in groups_from_amundsen if x not in ("")]

    for group_from_amundsen in groups_from_amundsen:
        split_values = group_from_amundsen.split(" - ")
        group_name, slack_channel = (
            split_values if len(split_values) == 2 else (split_values[0], None)
        )
        group = CorpGroup(
            id=group_name.lower(),
            owners=[],
            members=[],
            display_name=group_name,
            slack=slack_channel,
        )

        for event in group.generate_mcp(
            generation_config=CorpGroupGenerationConfig(
                override_editable=False, datahub_graph=graph
            )
        ):
            emit_event(graph, event)

    # emit owners
    amundsen_owner_map = dict(table_owners_from_s3_records)
    assert len(amundsen_owner_map) == len(
        table_owners_from_s3_records
    ), "duplicate owners"

    query = "select db, owner from dataplatform.database_owners where lower(owner) != 'unknown' and owner is not null and db is not null"
    result = run_dbx_query(query)

    db_map = {x.db: x.owner for x in result}

    urns = get_all_urns(context)

    for urn in urns:
        dataset_urn = urn["entity"]["urn"]
        catalog, database, table = get_db_and_table(dataset_urn)
        full_table = f"{database}.{table}"

        if database_filter and database_filter not in dataset_urn:
            continue

        if "datamodel" in dataset_urn:
            owner_to_add = "urn:li:corpGroup:dataengineering"

            ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
            owner_class_to_add = OwnerClass(owner=owner_to_add, type=ownership_type)
            ownership_to_add = OwnershipClass(owners=[owner_class_to_add])

            current_owners = ownership_to_add
            event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=current_owners,
            )
            emit_event(graph, event)

        elif full_table in amundsen_owner_map:
            amundsen_owner = amundsen_owner_map.get(full_table, None)
            if amundsen_owner:
                owner_id = amundsen_owner.split(" - ")[0].lower()
                owner_to_add = f"urn:li:corpGroup:{owner_id}"

                ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
                owner_class_to_add = OwnerClass(owner=owner_to_add, type=ownership_type)
                ownership_to_add = OwnershipClass(owners=[owner_class_to_add])

                current_owners = ownership_to_add
                event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=current_owners,
                )
                emit_event(graph, event)

        elif database in db_map:
            table_owner = db_map.get(database, None)

            if table_owner:
                owner_id = table_owner.lower()
                owner_to_add = f"urn:li:corpGroup:{owner_id}"

                ownership_type = OwnershipTypeClass.TECHNICAL_OWNER
                owner_class_to_add = OwnerClass(owner=owner_to_add, type=ownership_type)
                ownership_to_add = OwnershipClass(owners=[owner_class_to_add])

                current_owners = ownership_to_add
                event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=current_owners,
                )
                emit_event(graph, event)

    # prototype for Biztech and Marketing
    if EMIT_PERMISSIONS:
        query = """
                mutation createPolicy($input: PolicyUpdateInput!)
                {createPolicy(input: $input)}
                """

        for team_id, team_name in {
            "biztech": "Biztech",
            "marketing": "Marketing",
        }.items():
            variables = {
                "input": {
                    "type": "METADATA",
                    "name": team_name,
                    "state": "ACTIVE",
                    "description": "",
                    "privileges": ["VIEW_ENTITY_PAGE"],
                    "actors": {
                        "users": [],
                        "groups": [f"urn:li:corpGroup:{team_id}"],
                        "allUsers": False,
                        "allGroups": False,
                        "resourceOwners": False,
                    },
                    "resources": {
                        "filter": {
                            "criteria": [
                                {
                                    "field": "TYPE",
                                    "values": ["dataset"],
                                    "condition": "EQUALS",
                                },
                                {
                                    "field": "URN",
                                    "values": [
                                        f"urn:li:dataset:(urn:li:dataPlatform:databricks,samsara-biztech-poc.biztech-poc-metastore.dataplat.only_{team_id}_has_access.vehicle_make,PROD)",
                                        f"urn:li:dataset:(urn:li:dataPlatform:databricks,samsara-biztech-poc.biztech-poc-metastore.dataplat.only_{team_id}_has_access.joined_table,PROD)",
                                        f"urn:li:dataset:(urn:li:dataPlatform:databricks,samsara-biztech-poc.biztech-poc-metastore.dataplat.only_{team_id}_has_access.test_values,PROD)",
                                    ],
                                    "condition": "EQUALS",
                                },
                            ]
                        }
                    },
                }
            }

            result, err = run_graphql(graph, graphql_query=query, variables=variables)
            if err:
                context.log.error(err)

    log_op_duration(op_start_time)
    return 0
