import ast
import json
import os
import re
import subprocess
import time
from collections import Counter, OrderedDict, defaultdict, namedtuple
from dataclasses import astuple, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Literal

import boto3
import datahub.emitter.mce_builder as builder
import pandas as pd
from botocore.exceptions import ClientError
from dagster import AssetKey, Config, In, Nothing, Out, op
from datahub.emitter.mce_builder import (
    make_data_platform_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_domain_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import create_embed_mcp
from datahub.emitter.rest_emitter import DatahubRestEmitter
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
from datahub.metadata.com.linkedin.pegasus2avro.schema import MySqlDDL, SchemaField
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetProfileClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    EditableDatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    SchemaMetadataClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
    TagAssociationClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datamodel.ops.datahub.certified_tables import beta_tables, certified_tables
from datamodel.ops.datahub.datahub_views import datahub_views
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from sql_formatter.core import format_sql

from ..common.datahub_utils import (
    DataRecord,
    add_links,
    create_describe_dict,
    create_schema_fields,
    create_schema_fields_prior,
    database_filter,
    display_multiple_rows_scrollable,
    emit_event,
    emit_structured_properties,
    generate_metric_description,
    get_all_partitions_from_s3,
    get_all_urns,
    get_asset_specs_by_key,
    get_datahub_eks_s3_client,
    get_db_and_table,
    get_emitter,
    get_file_size_from_crc,
    get_graph,
    get_latest_merge_or_insert,
    get_latest_schema,
    get_latest_schema_from_crc,
    get_platform,
    get_repos,
    get_s3_bucket_from_table,
    get_s3_objects,
    get_s3_preview_data,
    get_untrustworthy_tables,
    list_files_in_s3_bucket,
    load_highlighted_queries,
    log_op_duration,
    run_dbx_query,
    run_graphql,
    safe_get,
    scrape_datahub_op_name,
    scrape_urn_metadata,
    send_datadog_count_metric,
    sync_tags_or_terms,
    transpose_single_row_scrollable,
)
from ..common.utils import (
    AWSRegions,
    find_line_number_in_code,
    get_datahub_env,
    get_datahub_token,
    get_gms_server,
    get_run_env,
    get_simple_field_path_from_v2_field_path,
    initialize_datadog,
    safe_get,
    send_datadog_count_metric,
    send_datadog_gauge_metric,
    slack_custom_alert,
)
from ..ops.datahub.constants import (
    biztech_dbs,
    datamodel_dbs,
    datamodel_silver_dbs,
    datapipelines_dbs,
    datastreams_dbs,
    edw_act_gold_dbs,
    edw_cs_gold_dbs,
    edw_csops_gold_dbs,
    edw_extract_gold_dbs,
    edw_fns_gold_dbs,
    edw_sales_gold_dbs,
    edw_salescomp_gold_dbs,
    edw_seops_gold_dbs,
    non_domain_dbs,
    rds_dbs,
)
from .datahub.backup_queries import backup_queries
from .datahub.data_contracts import contracts

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

initialize_datadog()

# daily
EMIT_DQ_CHECKS = True
EMIT_TABLE_LINEAGE = True

EMIT_DAGSTER_URLS_AND_TABLE_DESCRIPTIONS = True

EMIT_DATA_PROFILES = False  # intentionally False
EMIT_EMBEDDED_LINKS = True

if database_filter and database_filter not in ("datamodel", "metrics_repo", "auditlog"):
    EMIT_DAGSTER_URLS_AND_TABLE_DESCRIPTIONS = False

databases_with_invalid_glue_schemas = ["definitions"]

if os.getenv("MODE") in ("DRY_RUN", "LOCAL_PROD_CLUSTER_RUN"):
    EMIT_DQ_CHECKS = False
    EMIT_TABLE_LINEAGE = False
elif get_run_env() != "prod" and os.getenv("MODE") == "LOCAL_DATAHUB_RUN":
    EMIT_DQ_CHECKS = True
    EMIT_TABLE_LINEAGE = True
elif os.getenv("MODE") not in (
    "DRY_RUN",
    "LOCAL_PROD_CLUSTER_RUN",
    "LOCAL_DATAHUB_RUN",
):
    assert EMIT_DQ_CHECKS is True, "EMIT_DQ_CHECKS must be True in prod"
    assert not database_filter, "database_filter must be None in prod"

GOOGLE_FORMS_URL_PREFIX = "https://docs.google.com/forms/d/e/1FAIpQLSdlCB6Fn0luoQWVQYfG6xWld-ovV5Hjl3PclrlE6DAPezzSbA/viewform?usp=pp_url&entry.2018362552="


@op(out=Out(io_manager_key="in_memory_io_manager"))
def amundsen_metadata(context, result_code: int = 0) -> None:
    owner_map = {}
    asset_specs_by_key = get_asset_specs_by_key()

    context.log.info(asset_specs_by_key)

    asset_keys = ["__".join(k.path) for k in asset_specs_by_key.keys()]

    context.log.info(asset_keys)

    for asset_key, asset_spec in asset_specs_by_key.items():
        if len(asset_key.path) != 3:
            continue
        region, database, table = tuple(asset_key.path)
        if (
            database.startswith("datamodel_")
            or database
            in [
                "metrics_repo",
                "product_analytics",
                "product_analytics_staging",
                "dataengineering",
                "auditlog",
                "feature_store",
                "inference_store",
            ]
            and not (database.endswith("_dev") and not database.startswith("firmware"))
            and not table.startswith("dq_")
        ):
            # set_aws_credentials()
            owner_map[f"{database}.{table}"] = "DataEngineering"

            # Currently an entity can only have a single owner and it must be a team. So we
            # attribute ownership to the first team in the list of owners
            for asset_owner in asset_spec.owners:
                if (
                    asset_owner.split(":")[0] == "team"
                    and len(asset_owner.split(":")) == 2
                ):
                    owner_map[f"{database}.{table}"] = asset_owner.split("team:")[1]
                else:
                    owner_map[f"{database}.{table}"] = asset_owner
                break

            if database_filter and database_filter not in database:
                continue

            # export catalog metadata
            try:
                schema_values = asset_spec.metadata.get("schema", {})
                raw_json = json.dumps(schema_values)

                s3_client = boto3.client("s3")
                s3_client.put_object(
                    Body=raw_json,
                    Bucket="samsara-amundsen-metadata",
                    Key=f"staging/schemas/{database}/{table}.json",
                )
            except Exception as e:
                context.log.error(f"unable to export schema to S3 for {table}")
                context.log.error(e)

    context.log.info(owner_map)

    DATABASE = "auditlog" if get_datahub_env() == "prod" else "datamodel_dev"

    dim_dagster_assets_schema = {
        "asset_key": "STRING",
        "asset_owner": "STRING",
    }

    def _recreate_table(region=AWSRegions.US_WEST_2.value):
        run_dbx_query(f"DROP TABLE IF EXISTS {DATABASE}.dim_dagster_assets", region)
        time.sleep(5)

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.dim_dagster_assets (
                {", ".join([f"{k} {v}" for k, v in dim_dagster_assets_schema.items()])}
            )
            """

        run_dbx_query(create_table_query, region)

    obj_data_flattened = [
        (x, owner_map.get(".".join(x.split("__")[1:]))) for x in asset_keys
    ]

    values = ", ".join(
        [
            f"('{asset_key}', '{asset_owner}')"
            for asset_key, asset_owner in obj_data_flattened
        ]
    )

    insert_query = f"""
                INSERT OVERWRITE TABLE {DATABASE}.dim_dagster_assets ({",".join(dim_dagster_assets_schema.keys())})
                VALUES {values}
                """

    for region in ["us-west-2", "eu-west-1", "ca-central-1"]:
        try:
            run_dbx_query(insert_query, region)
        except Exception as e:
            context.log.error(f"error inserting into table: {e}")
            context.log.warning(
                "retrying after dropping and recreating table with updated schema"
            )
            try:
                _recreate_table(region)
                run_dbx_query(insert_query, region)

            except Exception as e:
                context.log.error(f"error inserting into table after recreating: {e}")

    raw_json = json.dumps(owner_map)

    s3_client.put_object(
        Body=raw_json,
        Bucket="samsara-amundsen-metadata",
        Key="staging/owners/dagster.json",
    )


@op(
    out=Out(io_manager_key="in_memory_io_manager"),
    required_resource_keys={"databricks_pyspark_step_launcher"},
)
def update_glue_column_descriptions(context) -> int:
    def update_column_description(
        context, spark, table_name, current_column_descriptions, column
    ):
        col_name = column["name"]
        col_comment = column.get("metadata", {}).get("comment", "")

        escaped_col_comment = col_comment.replace("'", "`").replace('"', '\\"')

        if col_comment == current_column_descriptions.get(col_name, ""):
            context.log.info(
                f"Current description ({escaped_col_comment}) already set for {col_name}, skipping"
            )
        else:
            spark.sql(
                f"ALTER TABLE {table_name} ALTER COLUMN {col_name} COMMENT '{escaped_col_comment}'"
            )
            context.log.info(
                f"Set new description: {escaped_col_comment}\n for column: {col_name}"
            )

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    asset_specs_by_key = get_asset_specs_by_key()

    for asset_key, asset_spec in asset_specs_by_key.items():
        if len(asset_key.path) != 3:
            continue
        region, database, table = tuple(asset_key.path)
        if (
            (
                database.startswith("datamodel_")
                or database
                in [
                    "product_analytics",
                    "product_analytics_staging",
                    "dataengineering",
                    "feature_store",
                    "inference_store",
                ]
            )
            and not (database.endswith("_dev") and not database.startswith("firmware"))
            and not table.startswith("dq_")
            and region == AWSRegions.US_WEST_2.value
        ):
            full_table_name = f"{database}.{table}"

            # sync schema
            schema_values = asset_spec.metadata.get("schema", {})

            glue_client = boto3.client("glue")
            try:
                glue_metadata = glue_client.get_table(
                    DatabaseName=database, Name=table
                )["Table"]["StorageDescriptor"]["Columns"]

                current_column_descriptions = {
                    x["Name"]: x.get("Comment", "") for x in glue_metadata
                }

                if schema_values:
                    for schema_col in schema_values:

                        dagster_description = schema_col.get("metadata", {}).get(
                            "comment", ""
                        )

                        if "metadata" not in schema_col:
                            context.log.error(
                                f"must embed comment in metadata for: {schema_col} in {database}.{table}"
                            )

                        if dagster_description != current_column_descriptions.get(
                            schema_col.get("name")
                        ):
                            context.log.info(
                                f"updating description for: {schema_col.get('name')}\n to: {dagster_description} on table {database}.{table}"
                            )
                            try:
                                update_column_description(
                                    context,
                                    spark,
                                    full_table_name,
                                    current_column_descriptions,
                                    schema_col,
                                )
                            except Exception as e:
                                context.log.info(e)
                                context.log.info(
                                    f"skipping column update for nested type: {schema_col.get('name')}"
                                )
                        else:
                            context.log.info(
                                f"no update needed for: {schema_col.get('name')}, matching column on table {database}.{table}"
                            )

                        full_table_name = f"{database}.{table}"

            except Exception as e:
                context.log.error(
                    f"unable to update column descriptions for {database}.{table}"
                )
                context.log.error(e)

    return 0


@op(
    out=Out(io_manager_key="in_memory_io_manager"),
    required_resource_keys={"databricks_pyspark_step_launcher"},
)
def delete_unused_tables(context) -> int:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    tables_to_delete = []  # fill in tables to drop here when running this locally

    for database_and_table_to_drop in tables_to_delete:
        context.log.info(f"dropping table {database_and_table_to_drop}")
        spark.sql(f"DROP TABLE IF EXISTS {database_and_table_to_drop}")

    return 0


emitter = DatahubRestEmitter(
    gms_server=gms_server, extra_headers={"Authorization": f"Bearer {token}"}
)
platform = "databricks"


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def extract_lineage_to_datahub(context) -> int:
    op_start_time = time.time()

    asset_specs_by_key = get_asset_specs_by_key()

    table_lineage_from_s3_records = dict(get_s3_objects("table_lineage"))

    # emit lineage
    def emit_table_lineage():

        bucket_name = "samsara-amundsen-metadata"
        prefix = "staging/lineage/"

        lineage_map = defaultdict(set)

        # Unity Catalog lineage
        lineage_from_unity = run_dbx_query(
            query="""
            SELECT
                concat(source_table_schema, '.', source_table_name) as source_table,
                concat(target_table_schema, '.', target_table_name) as target_table
            from
                system.access.table_lineage
            where
                source_table_catalog = 'default'
                and target_table_catalog = 'default'
                and source_type in ('TABLE', 'VIEW')
                and event_time >= date_sub(current_date(), IF(target_type = 'VIEW', 365, 30))
            group by
                1,
                2
            order by
                1,
                2
            """,
        )

        tables_in_graph = [
            get_db_and_table(x["entity"]["urn"], return_type="string")
            for x in get_all_urns(context)
        ]

        for row in lineage_from_unity:
            if (
                row.target_table in tables_in_graph
                and row.source_table in tables_in_graph
            ):
                lineage_map[row.target_table].add(row.source_table)

        # Amundsen lineage
        for upstream_table, downstream_tables in table_lineage_from_s3_records.items():
            if database_filter and database_filter not in upstream_table:
                continue

            if not downstream_tables:
                continue

            for downstream_table in downstream_tables:
                lineage_map[downstream_table].add(upstream_table)

        data = list_files_in_s3_bucket(context, bucket_name, prefix, lookback_days=3)

        # SQL lineage
        for downstream, upstream_list_of_lists in data.items():
            if downstream.split(".")[0].endswith("_dev"):
                continue

            if database_filter and database_filter not in downstream:
                continue

            distinct_upstreams = set()
            for upstream_list in upstream_list_of_lists:
                for upstream_table in upstream_list:
                    if any(
                        upstream_table.startswith(prefix)
                        for prefix in ["kinesisstats_history", "datastreams_history"]
                    ):
                        upstream_table = upstream_table.replace("_history.", ".")
                    distinct_upstreams.add(upstream_table)

            lineage_map[downstream].update(distinct_upstreams)

        # Dagster lineage
        for asset_key, _ in asset_specs_by_key.items():
            if len(asset_key.path) != 3:
                continue
            region, database, table = tuple(asset_key.path)
            if (
                (
                    database.startswith("datamodel_")
                    or database.startswith("firmware_dev")
                    or database
                    in [
                        "product_analytics",
                        "product_analytics_staging",
                        "dataengineering",
                        "feature_store",
                        "inference_store",
                    ]
                )
                and not (
                    database.endswith("_dev") and not database.startswith("firmware")
                )
                and not table.startswith("dq_")
                and region == AWSRegions.US_WEST_2.value
            ):
                if database_filter and database_filter not in database:
                    continue

                asset_key = AssetKey([AWSRegions.US_WEST_2.value, database, table])
                try:
                    asset_upstream_tables = [
                        f"{x.asset_key.path[-2]}.{x.asset_key.path[-1]}"
                        for x in asset_specs_by_key[asset_key].deps
                    ]
                except Exception as e:
                    context.log.info(e)
                    context.log.info(f"no upstream tables for {asset_key}")
                    continue
                asset_upstream_tables = [
                    x.replace(".dq_", ".") for x in asset_upstream_tables
                ]

                for upstream_table in asset_upstream_tables:
                    lineage_map[f"{database}.{table}"].add(upstream_table)

        for downstream, distinct_upstreams in lineage_map.items():

            upstream_urns = [
                builder.make_dataset_urn_with_platform_instance(
                    platform="dbt" if x.startswith("edw") else platform,
                    name=x,
                    platform_instance=y,
                    env="PROD",
                )
                for (x, y) in zip(
                    distinct_upstreams,
                    [
                        "databricks" if x.startswith("edw") else None
                        for x in distinct_upstreams
                    ],
                )
            ]

            downstream_urn = builder.make_dataset_urn_with_platform_instance(
                platform="dbt" if downstream.startswith("edw") else platform,
                name=downstream,
                platform_instance=(
                    "databricks" if downstream.startswith("edw") else None
                ),
                env="PROD",
            )

            if downstream.startswith("edw"):
                context.log.info(
                    f"emitting lineage from {upstream_urns} to {downstream_urn}"
                )

            lineage_mce_1 = builder.make_lineage_mce(upstream_urns, downstream_urn)

            emit_event(emitter, lineage_mce_1, event_type="mce")

        lineage_map_dict = {k: list(v) for k, v in lineage_map.items()}

        # dump lineage to s3
        s3_client = boto3.client("s3")
        s3_client.put_object(
            Body=json.dumps(lineage_map_dict),
            Bucket="samsara-datahub-metadata",
            Key=f"metadata/exports/{get_datahub_env()}/lineage/lineage_map.json",
        )

    if EMIT_TABLE_LINEAGE:
        emit_table_lineage()

    op_duration = time.time() - op_start_time

    send_datadog_gauge_metric(
        metric=f"dagster.{METRIC_PREFIX}.job.run.duration",
        value=op_duration,
        tags=[f"op:{scrape_datahub_op_name()}"],
    )

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def delete_stale_entities(context) -> int:

    op_start_time = time.time()

    graph = DataHubGraph(
        config=DatahubClientConfig(
            server=gms_server,
            extra_headers={"Authorization": f"Bearer {token}"},
        )
    )

    urns = get_all_urns(context)
    stale_urns_to_delete = []

    PLATFORMS_WITHOUT_GC = ["tableau", "dbt"]

    # delete entities without a latest publish in last 3 days
    for urn in urns:
        urn = urn["entity"]["urn"]
        catalog, database, table = get_db_and_table(urn)

        platform = get_platform(urn)

        current_properties = graph.get_aspect(
            entity_urn=urn, aspect_type=DatasetPropertiesClass
        )

        if not current_properties:
            last_updated = None
        else:
            last_updated_glue = current_properties.get("customProperties", {}).get(
                "latest_glue_ingestion_timestamp_utc",
            )

            last_updated_unity = current_properties.get("customProperties", {}).get(
                "latest_unity_ingestion_timestamp_utc",
            )

            last_updated_dbt = current_properties.get("customProperties", {}).get(
                "latest_dbt_ingestion_timestamp_utc",
            )

            last_updated = last_updated_glue or last_updated_unity or last_updated_dbt

        if last_updated and platform not in PLATFORMS_WITHOUT_GC:
            last_updated = int(last_updated)
            now = int(datetime.now(timezone.utc).timestamp())

            # if table not seen in UC for 3 hours, delete (this runs after UC ingestion so is safe from race conditions)
            if now - last_updated > 60 * 60 * 3:
                context.log.info(f"will delete {urn} since it is stale")
                stale_urns_to_delete.append(urn)
        elif platform not in PLATFORMS_WITHOUT_GC:
            stale_urns_to_delete.append(urn)

    SAFETY_VALVE_THRESHOLD = 0.15

    if len(stale_urns_to_delete) / len(urns) > SAFETY_VALVE_THRESHOLD:
        context.log.warning(
            f"More than {int(100*SAFETY_VALVE_THRESHOLD)}% ({len(stale_urns_to_delete)} urns), which seems anomalous: NOT DELETING"
        )
        if get_datahub_env() == "prod":
            slack_custom_alert(
                "alerts-data-tools",
                f"More than {int(100*SAFETY_VALVE_THRESHOLD)}% of urns are being deleted, which seems anomalous: NOT DELETING",
            )
    else:
        context.log.info(f"deleting {len(stale_urns_to_delete)} stale urns")
        for urn in stale_urns_to_delete:
            graph.delete_entity(urn=urn, hard=True)
            context.log.info(
                f"Deleted urn because it has no reported ingestions in last 3 days {urn}"
            )
            send_datadog_count_metric(
                metric=f"{METRIC_PREFIX}.stale_urns_deleted",
                tags=[f"database:{database}"],
            )

    tables_to_delete = [
        "datamodel_core_silver.dim_organizations",
        "datamodel_core_silver.hacksara_activity_agg",
        "datamodel_core_silver.stg_device_heartbeat_daily",
        "datamodel_core_bronze.hacksara_activity_agg_output",
        "datamodel_dev.fct_safety_events",
        "datamodel_dev.stg_safety_events",
        "datamodel_dev.dim_eld_relevant_devices",
        "datamodel_dev.stg_activity_events",
        "datamodel_telematics_silver_dev",
        "datamodel_dev.stg_eld_relevant_devices",
        # doesn't exist, or is not a Delta table
        "connectedworker.streams_metrics_v1",
        "dispatchdb.shardable_dispatch_routes",
    ]

    for table in tables_to_delete:
        delete_dataset_urn = builder.make_dataset_urn(name=table, platform="databricks")

        graph.delete_entity(urn=delete_dataset_urn, hard=True)

        context.log.info(f"Deleted dataset {delete_dataset_urn}")

    urns_to_delete = [
        "urn:li:assertion:27be864dd1d22ba9d3a86138a875bd9a76b3cac258292b566bddfbcd96a5db9b"
    ]

    for urns in urns_to_delete:

        graph.delete_entity(urn=urns, hard=True)

        context.log.info(f"Deleted urn {urns}")

    # CAREFUL WITH THIS - ONLY RUN IN DEV
    DELETE_DBS = True

    dbs_to_delete = None

    urns = get_all_urns(context)

    if DELETE_DBS and dbs_to_delete:
        for db_to_delete in dbs_to_delete:
            for urn in urns:
                urn = urn["entity"]["urn"]
                catalog, database, table = get_db_and_table(urn)

                if database == db_to_delete:
                    graph.delete_entity(urn=urn, hard=True)
                    context.log.info(f"Deleted urn {urn}")

    # delete urns without s3 paths (just for rds right now)
    urns = get_all_urns(context)

    # remove urns with invalid table names

    invalid_table_prefixes = [
        "test_",
        "raman_",
        "moonchu_",
        "moon_",
        "bdonecker_",
        "tpassaro",
        "temp_",
        "temporary",
        "eli_",
        "jack_",
        "lucas_",
        "meenu_",
        "tmp_",
        "udita_",
        "agovan_",
        "alice_",
        "chenyu_",
        "joyce_",
        "abrahm_",
        "gil_",
        "jk_",
    ]

    for urn in urns:
        urn = urn["entity"]["urn"]
        catalog, database, table = get_db_and_table(urn)

        for table_prefix in invalid_table_prefixes:
            if table.startswith(table_prefix):
                context.log.info(f"Deleting urn {urn} since it has invalid prefix")
                graph.delete_entity(urn=urn, hard=True)

    # delete missing s3 paths for rds
    for urn in urns:
        urn = urn["entity"]["urn"]

        catalog, database, table = get_db_and_table(urn)

        if database not in [*rds_dbs]:
            continue

        current_properties = graph.get_aspect(
            entity_urn=urn, aspect_type=DatasetPropertiesClass
        )

        if not current_properties:
            s3_location = None
        else:
            s3_location = current_properties.get("customProperties", {}).get(
                "location",
                current_properties.get("customProperties", {}).get("storage_location"),
            )

        if not s3_location:
            graph.delete_entity(urn=urn, hard=True)
            context.log.info(f"Deleting urn {urn} since it has no s3 path")

    op_duration = time.time() - op_start_time

    send_datadog_gauge_metric(
        metric=f"dagster.{METRIC_PREFIX}.job.run.duration",
        value=op_duration,
        tags=[f"op:{scrape_datahub_op_name()}"],
    )

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_table_descriptions_to_datahub(context) -> int:
    op_start_time = time.time()
    asset_specs_by_key = get_asset_specs_by_key()
    graph = get_graph(context)
    urns = get_all_urns(context)

    table_descriptions_from_s3_records = get_s3_objects("table_descriptions")

    table_programmatic_descriptions_from_s3_records = get_s3_objects(
        "table_programmatic_descriptions"
    )

    table_programmatic_descriptions_from_s3_records_dict = dict(
        table_programmatic_descriptions_from_s3_records
    )

    table_descriptions_from_s3_records_dict = dict(table_descriptions_from_s3_records)

    from datamodel.common import datahub_descriptions

    datahub_descriptions_dict = datahub_descriptions.descriptions

    context.log.info(f"datahub_descriptions_dict: {datahub_descriptions_dict}")

    for urn in urns:

        dataset_urn = urn["entity"]["urn"]

        catalog, database, table = get_db_and_table(dataset_urn)

        if database_filter and database_filter not in database:
            continue

        table_name = f"{database}.{table}"
        table_name_with_catalog = f"{catalog or 'default'}.{database}.{table}"

        table_description = None

        if table_name_with_catalog in datahub_descriptions_dict:
            table_description = datahub_descriptions_dict[table_name_with_catalog][
                "description"
            ]
        elif table_name in table_descriptions_from_s3_records_dict:
            table_description = table_descriptions_from_s3_records_dict[table_name]

        if table_description:

            programmatic_descriptions = (
                table_programmatic_descriptions_from_s3_records_dict.get(
                    f"{database}.{table}"
                )
            )

            if programmatic_descriptions:
                for programmatic_description in programmatic_descriptions:
                    table_description += f"\n\n\n**{programmatic_description.get('description_source')}**"

                    description = programmatic_description.get("description", "")
                    for link in description.split("\n"):
                        table_description += f"\n\n{link}"

            current_properties = graph.get_aspect(
                entity_urn=dataset_urn, aspect_type=EditableDatasetPropertiesClass
            )

            if current_properties:
                current_properties.description = table_description
            else:
                current_properties = EditableDatasetPropertiesClass(
                    description=table_description
                )

            metadata_event_properties = MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=current_properties,
            )

            emit_event(emitter, metadata_event_properties)

    tables_in_graph = [
        get_db_and_table(x["entity"]["urn"], return_type="string")
        for x in get_all_urns(context)
    ]

    for tbl, contract_url in contracts.items():
        if tbl not in tables_in_graph:
            continue

        dataset_urn = builder.make_dataset_urn(platform, tbl)

        add_links(context, graph, dataset_urn, {"Data Contract": contract_url})

    if EMIT_DAGSTER_URLS_AND_TABLE_DESCRIPTIONS:
        tables_in_graph = [
            get_db_and_table(x["entity"]["urn"], return_type="string")
            for x in get_all_urns(context)
        ]

        # emit Dagster schemas
        for asset_key, asset_spec in asset_specs_by_key.items():
            if len(asset_key.path) != 3:
                continue
            region, database, table = tuple(asset_key.path)

            if (
                (
                    database.startswith("datamodel_")
                    or database.startswith("firmware_dev")
                    or database
                    in [
                        "metrics_repo",
                        "auditlog",
                        "product_analytics",
                        "product_analytics_staging",
                        "dataengineering",
                        "feature_store",
                        "inference_store",
                    ]
                )
                and not (
                    database.endswith("_dev") and not database.startswith("firmware")
                )
                and not table.startswith("dq_")
                and region == AWSRegions.US_WEST_2.value
                and f"{database}.{table}" in tables_in_graph
            ):
                if database_filter and database_filter not in database:
                    continue

                full_table_name = f"{database}.{table}"

                graph = DataHubGraph(
                    config=DatahubClientConfig(
                        server=gms_server,
                        extra_headers={"Authorization": f"Bearer {token}"},
                    )
                )

                dataset_urn = builder.make_dataset_urn(platform, full_table_name)

                current_properties = graph.get_aspect(
                    entity_urn=dataset_urn, aspect_type=EditableDatasetPropertiesClass
                )

                source_code_link = asset_spec.metadata.get("code_location", "")

                links_to_add = {
                    "Databricks URL": f"https://samsara-dev-us-west-2.cloud.databricks.com/explore/data/default/{database}/{table}?o=5924096274798303#",
                    "Dagster URL": f"https://dagster.internal.samsara.com/assets/us-west-2/{database}/{table}",
                    "Submit Feedback": f"{GOOGLE_FORMS_URL_PREFIX}{dataset_urn}",
                }

                if source_code_link:
                    if (
                        get_datahub_env() == "prod"
                        and "dataweb" not in source_code_link
                    ):
                        source_code_link = "dagster/projects/" + source_code_link

                    file_path, line_number = source_code_link.split(", line ")
                    if "datamodel" in file_path:
                        github_url = f"https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/{file_path}#L{int(line_number)}"
                    elif "dataweb" in file_path:
                        github_url = f"https://github.com/samsara-dev/backend/blob/master/dataplatform/{file_path}#L{int(line_number)}"  ##proper link for dataweb
                    links_to_add["GitHub URL"] = github_url

                add_links(context, graph, dataset_urn, links_to_add)

                cur_description = asset_spec.metadata.get("description", "")

                if database == "metrics_repo":
                    metadata = asset_spec.metadata

                    try:
                        cur_description = generate_metric_description(
                            context, database, table, metadata
                        )
                    except Exception as e:
                        context.log.error(e)
                        cur_description = None

                    if not cur_description:
                        context.log.error(
                            f"no metric description could be pulled for {database}.{table}"
                        )
                        continue
                    metrics_links = links_to_add.copy()
                    metrics_links["Databricks URL"] = metrics_links["Databricks URL"]
                    metrics_links["Dagster URL"] = metrics_links["Dagster URL"]
                    add_links(context, graph, dataset_urn, metrics_links)

                if current_properties:
                    current_properties.description = cur_description
                else:
                    current_properties = EditableDatasetPropertiesClass(
                        description=cur_description
                    )

                metadata_event_properties = MetadataChangeProposalWrapper(
                    entityUrn=dataset_urn,
                    aspect=current_properties,
                )

                emit_event(emitter, metadata_event_properties)

    op_duration = time.time() - op_start_time

    send_datadog_gauge_metric(
        metric=f"dagster.{METRIC_PREFIX}.job.run.duration",
        value=op_duration,
        tags=[f"op:{scrape_datahub_op_name()}"],
    )

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_column_descriptions_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)

    table_column_descriptions_from_s3_records = get_s3_objects(
        "table_column_descriptions"
    )

    asset_specs_by_key = get_asset_specs_by_key()

    tables_in_graph = [
        get_db_and_table(x["entity"]["urn"], return_type="string")
        for x in get_all_urns(context)
    ]

    from datamodel.common import datahub_descriptions

    datahub_descriptions_dict = datahub_descriptions.descriptions

    EMIT_DAGSTER_NESTED_COLUMN_DESCRIPTIONS = True

    if EMIT_DAGSTER_NESTED_COLUMN_DESCRIPTIONS:
        # for nested decriptions only

        for asset_key, asset_spec in asset_specs_by_key.items():
            if len(asset_key.path) != 3:
                continue
            region, database, table = tuple(asset_key.path)
            if database_filter and database_filter not in database:
                continue

            if (
                (
                    database.startswith("datamodel_")
                    or database.startswith("firmware_dev")
                    or database
                    in [
                        "metrics_repo",
                        "auditlog",
                        "product_analytics",
                        "product_analytics_staging",
                        "dataengineering",
                        "feature_store",
                        "inference_store",
                    ]
                )
                and not (
                    database.endswith("_dev") and not database.startswith("firmware")
                )
                and not table.startswith("dq_")
                and region == AWSRegions.US_WEST_2.value
                and f"{database}.{table}" in tables_in_graph
            ):
                table_name = f"{database}.{table}"

                asset_context = asset_spec.metadata
                schema = asset_context.get("schema", [])
                if not schema:
                    continue

                dataset_urn = builder.make_dataset_urn(platform, table_name)

                current_editable_schema_metadata = graph.get_aspect(
                    entity_urn=dataset_urn,
                    aspect_type=SchemaMetadataClass,
                )

                if current_editable_schema_metadata:
                    for fieldInfo in current_editable_schema_metadata.fields:

                        field_name = get_simple_field_path_from_v2_field_path(
                            fieldInfo.fieldPath
                        )

                        schema_element = schema
                        nested_field_path = field_name.split(".")
                        field_name = nested_field_path.pop(0)
                        schema_element = [
                            x for x in schema_element if x["name"] == field_name
                        ]
                        if schema_element:
                            schema_element = schema_element[0]
                        else:
                            continue

                        while nested_field_path:
                            field_name = nested_field_path.pop(0)

                            if (
                                schema_element
                                and schema_element.get("type", {}).get("type")
                                == "array"
                            ):
                                schema_element_subarray = (
                                    schema_element.get("type", {})
                                    .get("elementType", {})
                                    .get("fields", [])
                                )
                                schema_element = next(
                                    (
                                        x
                                        for x in schema_element_subarray
                                        if x["name"] == field_name
                                    ),
                                    None,
                                )
                                if schema_element is None:
                                    context.log.error(
                                        f"schema_element is None for table: {table_name} field: {field_name}"
                                    )
                                    continue
                            elif (
                                schema_element
                                and schema_element.get("type", {}).get("type")
                                == "struct"
                            ):
                                schema_element_subarray = schema_element.get(
                                    "type", {}
                                ).get("fields", {})
                                schema_element = next(
                                    (
                                        x
                                        for x in schema_element_subarray
                                        if x["name"] == field_name
                                    ),
                                    None,
                                )
                                if schema_element is None:
                                    context.log.error(
                                        f"schema_element is None for table: {table_name} field: {field_name}"
                                    )
                                    continue
                            elif schema_element:
                                schema_element = next(
                                    (
                                        x
                                        for x in schema_element
                                        if x["name"] == field_name
                                    ),
                                    None,
                                )
                                if schema_element is None:
                                    context.log.error(
                                        f"schema_element is None for table: {table_name} field: {field_name}"
                                    )
                                    continue

                        if schema_element and isinstance(
                            schema_element.get("metadata", {}), dict
                        ):
                            col_description = schema_element.get("metadata", {}).get(
                                "comment", ""
                            )
                        else:
                            # Handle the case where schema_element is not a dictionary
                            col_description = ""
                            context.log.error(
                                f"schema_element is not a dictionary: {schema_element} for table: {table_name}"
                            )

                        if not col_description:
                            continue

                        col_description_multiline = json.dumps(col_description)

                        graphql_query = f"""
                            mutation updateDescription {{
                                updateDescription(
                                    input: {{
                                    description: {col_description_multiline},
                                    resourceUrn:"{dataset_urn}",
                                    subResource: "{fieldInfo.fieldPath}"
                                    subResourceType:DATASET_FIELD
                                    }}
                                        )
                                }}
                        """

                        result, err = run_graphql(graph, graphql_query)
                        if err:
                            context.log.error(err)

    for table_name, schema in table_column_descriptions_from_s3_records:

        if database_filter and database_filter not in table_name:
            continue

        dataset_urn = builder.make_dataset_urn(platform, table_name)

        current_editable_schema_metadata = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=SchemaMetadataClass,
        )

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.fields:
                col_description = schema.get(
                    get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath),
                    "",
                )

                field_info_description_comparison = (
                    "" if not fieldInfo.description else fieldInfo.description
                )

                if field_info_description_comparison == col_description:
                    continue

                col_description_multiline = json.dumps(col_description)

                graphql_query = f"""
                    mutation updateDescription {{
                        updateDescription(
                            input: {{
                            description: {col_description_multiline},
                            resourceUrn:"{dataset_urn}",
                            subResource: "{fieldInfo.fieldPath}"
                            subResourceType:DATASET_FIELD
                            }}
                                )
                        }}
                """

                result, err = run_graphql(graph, graphql_query=graphql_query)
                if err:
                    context.log.error(err)

    for full_table_name, v in datahub_descriptions_dict.items():

        schema = v.get("schema", {})

        context.log.info(f"full_table_name: {full_table_name}")
        context.log.info(f"schema: {schema}")

        if database_filter and database_filter not in full_table_name:
            continue

        catalog, database, table = full_table_name.split(".")

        table_name = f"{database}.{table}"

        if catalog != "default" and catalog:
            dataset_urn = builder.make_dataset_urn(platform, full_table_name)
        else:
            dataset_urn = builder.make_dataset_urn(platform, table_name)

        current_editable_schema_metadata = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=SchemaMetadataClass,
        )

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.fields:
                col_description = schema.get(
                    get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath),
                    "",
                )

                field_info_description_comparison = (
                    "" if not fieldInfo.description else fieldInfo.description
                )

                if field_info_description_comparison == col_description:
                    continue

                col_description_multiline = json.dumps(col_description)

                graphql_query = f"""
                    mutation updateDescription {{
                        updateDescription(
                            input: {{
                            description: {col_description_multiline},
                            resourceUrn:"{dataset_urn}",
                            subResource: "{fieldInfo.fieldPath}"
                            subResourceType:DATASET_FIELD
                            }}
                                )
                        }}
                """

                result, err = run_graphql(graph, graphql_query=graphql_query)
                if err:
                    context.log.error(err)

    log_op_duration(op_start_time)

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_domains_and_views_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)
    emitter = get_emitter()

    sub_domain_kinesis_stats = make_domain_urn("kinesis_stats")
    sub_domain_rds = make_domain_urn("rds")
    sub_domain_dynamodb = make_domain_urn("dynamodb")

    for domain_id, domain_name, domain_description in [
        (
            "datamodel_all",
            "Product Data Model",
            "Entities related to the product data model (all layers)",
        ),
        (
            "replication",
            "Replicated Product Data",
            "Raw data replicated from source systems. Should not be queried directly",
        ),
        (
            "engagement",
            "Engagement Data",
            "Raw engagement data replicated from Mixpanel",
        ),
        (
            "metrics_repo",
            "Metrics Repo",
            "Trusted metrics from the Samsara Metrics Repository",
        ),
        (
            "notebooks",
            "Team-managed Tables",
            "Tables created via team-managed Notebooks / Workflows. These tables do not have quality or freshness SLOs.",
        ),
        (
            "mapping_tables",
            "Mapping Tables",
            "Mapping tables (e.g. definitions)",
        ),
        (
            "datastreams",
            "Datastreams",
            "Custom Team-specific Logging Data",
        ),
        (
            "datapipelines",
            "Datapipelines",
            "Custom Pre-computed Reports, usually customer facing",
        ),
        (
            "biztech",
            "BizTech",
            "BizTech Data Model",
        ),
        (
            "samsara_business_data",
            "Samsara Business Data",
            "Samsara Business Systems Data",
        ),
        (
            "non_govramp",
            "Non-GovRamp Product Data",
            "The non-govramp catalog holds all tables that should be available to non-RnD users",
        ),
    ]:

        domain_urn = make_domain_urn(domain_id)

        domain_properties_aspect = DomainPropertiesClass(
            name=domain_name,
            description=domain_description,
        )

        domain_event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
            entityType="domain",
            changeType=ChangeTypeClass.UPSERT,
            entityUrn=domain_urn,
            aspect=domain_properties_aspect,
        )

        context.log.info(
            f"emitting domain {domain_id} {domain_name} {domain_description}"
        )

        try:
            emit_event(emitter, domain_event)
        except Exception as e:
            context.log.error(
                f"error emitting domain {domain_id} {domain_name} {domain_description}: {e}"
            )
            raise e

        for domain_id, domain_name, domain_description, parent_domain in [
            (
                "s3bigstats",
                "s3bigstats",
                "Large Payloads associated with Kinesis Stats",
                make_domain_urn("replication"),
            ),
            (
                "datamodel",
                "Product Data Model - Gold",
                "Entities related to the product data model (Gold Layer). Authoritative and trustworthy.",
                make_domain_urn("datamodel_all"),
            ),
            (
                "datamodel_silver",
                "Product Data Model - Silver",
                "Entities related to the product data model (silver layer). Trustworthy but use with caution.",
                make_domain_urn("datamodel_all"),
            ),
            (
                "datamodel_bronze",
                "Product Data Model - Bronze",
                "Entities related to the product data model (bronze layer). Query at your own discretion.",
                make_domain_urn("datamodel_all"),
            ),
            (
                "samsara_business_data_bronze",
                "Samsara Business Data - Bronze",
                "Entities related to the samsara business data (bronze layer). Only allowed for bted service principals.",
                make_domain_urn("samsara_business_data"),
            ),
            (
                "samsara_business_data_silver",
                "Samsara Business Data - Silver",
                "Entities related to the samsara business data (silver layer). Authoritative and trustworthy. Accessible widely across org",
                make_domain_urn("samsara_business_data"),
            ),
            (
                "samsara_business_data_gold",
                "Samsara Business Data - Gold",
                "Entities related to the samsara business data (gold layer). Authoritative and trustworthy. Aggregated Data models for specific business domains ",
                make_domain_urn("samsara_business_data"),
            ),
            (
                "samsara_business_data_sterling",
                "Samsara Business Data - Sterling",
                "Entities related to the samsara business data (sterling layer). Query at your own discretion, limited access provided for edge cases not served via silver or gold ",
                make_domain_urn("samsara_business_data"),
            ),
        ]:
            sub_domain = make_domain_urn(domain_id)

            domain_properties_aspect = DomainPropertiesClass(
                name=domain_name,
                description=domain_description,
                parentDomain=parent_domain,
            )

            domain_event_datamodel_silver: MetadataChangeProposalWrapper = (
                MetadataChangeProposalWrapper(
                    entityType="domain",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=sub_domain,
                    aspect=domain_properties_aspect,
                )
            )

            emit_event(emitter, domain_event_datamodel_silver)

            for domain_id, domain_name, domain_description, parent_domain in [
                (
                    "samsara_business_data_csops",
                    "Customer Support",
                    "Customer Support",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_customer_success",
                    "Customer Success",
                    "Customer Success",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_fns",
                    "Finance & Strategy",
                    "Finance & Strategy",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_accounting",
                    "Accounting",
                    "Accounting",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_extracts",
                    "Extracts",
                    "Outbound extracts from Enterprise Data Warehouse",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_sales_eng",
                    "Sales Engineering",
                    "Sales Engineering",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_salescomp",
                    "Salescomp",
                    "Salescomp",
                    make_domain_urn("samsara_business_data_gold"),
                ),
                (
                    "samsara_business_data_sales",
                    "Sales",
                    "Sales",
                    make_domain_urn("samsara_business_data_gold"),
                ),
            ]:
                sub_domain = make_domain_urn(domain_id)

                domain_properties_aspect = DomainPropertiesClass(
                    name=domain_name,
                    description=domain_description,
                    parentDomain=parent_domain,
                )

                domain_event_datamodel_silver: MetadataChangeProposalWrapper = (
                    MetadataChangeProposalWrapper(
                        entityType="domain",
                        changeType=ChangeTypeClass.UPSERT,
                        entityUrn=sub_domain,
                        aspect=domain_properties_aspect,
                    )
                )

                emit_event(emitter, domain_event_datamodel_silver)

    domain_properties_aspect_ks = DomainPropertiesClass(
        name="Kinesis Stats",
        description="Kinesis Stats diagnostics data. Query at your own discretion.",
        parentDomain=make_domain_urn("replication"),
    )

    domain_event_ks: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=sub_domain_kinesis_stats,
        aspect=domain_properties_aspect_ks,
    )

    emit_event(emitter, domain_event_ks)

    domain_properties_aspect_rds = DomainPropertiesClass(
        name="RDS",
        description="RDS database replication. Query at your own discretion.",
        parentDomain=make_domain_urn("replication"),
    )

    domain_event_rds: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=sub_domain_rds,
        aspect=domain_properties_aspect_rds,
    )

    emit_event(emitter, domain_event_rds)

    domain_properties_aspect_dynamo = DomainPropertiesClass(
        name="DynamoDB",
        description="DynamoDB database replication. Query at your own discretion.",
        parentDomain=make_domain_urn("replication"),
    )

    domain_event_dynamo: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityType="domain",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=sub_domain_dynamodb,
        aspect=domain_properties_aspect_dynamo,
    )

    emit_event(emitter, domain_event_dynamo)

    urns = get_all_urns(context)

    for dataset_urn in urns:
        dataset_urn = dataset_urn["entity"]["urn"]
        catalog, database, table = get_db_and_table(dataset_urn)

        if database_filter and database_filter not in database:
            continue

        if catalog and catalog == "edw_bronze":
            domain = "samsara_business_data_bronze"
        elif catalog and catalog == "non_govramp_customer_data":
            domain = "non_govramp"
        elif catalog and catalog == "edw":
            if database.endswith("silver"):
                domain = "samsara_business_data_silver"
            elif database in edw_csops_gold_dbs:
                domain = "samsara_business_data_csops"
            elif database in edw_extract_gold_dbs:
                domain = "samsara_business_data_extracts"
            elif database in edw_cs_gold_dbs:
                domain = "samsara_business_data_customer_success"
            elif database in edw_fns_gold_dbs:
                domain = "samsara_business_data_fns"
            elif database in edw_act_gold_dbs:
                domain = "samsara_business_data_accounting"
            elif database in edw_seops_gold_dbs:
                domain = "samsara_business_data_sales_eng"
            elif database in edw_salescomp_gold_dbs:
                domain = "samsara_business_data_salescomp"
            elif database in edw_sales_gold_dbs:
                domain = "samsara_business_data_sales"
            elif database.endswith("gold"):
                domain = "samsara_business_data_gold"
            elif database.endswith("sterling"):
                domain = "samsara_business_data_sterling"
            else:
                domain = "samsara_business_data"
        elif (
            database.startswith("datamodel_")
            or database.startswith("kinesisstats")
            or database in rds_dbs
            or database in non_domain_dbs
            or database in datastreams_dbs
            or database in datapipelines_dbs
            or database in biztech_dbs
            or database.startswith("mixpanel")
            or database
            in [
                "dynamodb",
                "definitions",
                "s3bigstats",
                "metrics_repo",
                "product_analytics",
                "product_analytics_staging",
                "dataengineering",
                "feature_store",
                "inference_store",
            ]
        ):
            domain = None
            if database in datamodel_dbs:
                domain = "datamodel"
            elif database in biztech_dbs:
                domain = "biztech"
            elif database.startswith("kinesisstats"):
                domain = "kinesis_stats"
            elif database in rds_dbs:
                domain = "rds"
            elif database.startswith("mixpanel"):
                domain = "engagement"
            elif database == "metrics_repo":
                domain = "metrics_repo"
            elif database in non_domain_dbs:
                domain = "notebooks"
            elif database == "dynamodb":
                domain = "dynamodb"
            elif database == "definitions":
                domain = "mapping_tables"
            elif database in datastreams_dbs:
                domain = "datastreams"
            elif database in datapipelines_dbs:
                domain = "datapipelines"
            elif database == "s3bigstats":
                domain = "s3bigstats"
            elif database.endswith("silver") or database == "product_analytics_staging":
                domain = "datamodel_silver"
            elif database.endswith("bronze"):
                domain = "datamodel_bronze"
        #  TODO:  (update this condition if we enable other domains from tableau)
        elif "tableau" in dataset_urn.lower():
            domain = "samsara_business_data"

        if not domain:
            continue

        query = f"""
        mutation setDomain {{
            setDomain(domainUrn: "urn:li:domain:{domain}",
            entityUrn: "{dataset_urn}")
        }}
        """

        result, err = run_graphql(graph, query)
        if err:
            context.log.error(f"unable to set domain for {dataset_urn}")
            context.log.error(err)

    # emit datahub views
    graphql_query = """
        query listGlobalViews($start: Int!, $count: Int!, $query: String) {
                listGlobalViews(input: { start: $start, count: $count, query: $query }) {
                    views {
                    urn
                    name
                    }
                }
            }
    """

    current_views, err = run_graphql(
        graph, graphql_query, variables={"start": 0, "count": 1000}
    )

    current_views = {
        x["name"]: x["urn"] for x in current_views["listGlobalViews"]["views"]
    }

    for view_name, view_info in datahub_views.items():

        if view_name in current_views:
            query = """mutation updateView($urn: String!, $input: UpdateViewInput!) {
                                    updateView(urn: $urn, input: $input) {
                                        urn
                                        type
                                        viewType
                                        name
                                        description
                                        definition {
                                        entityTypes
                                        filter {
                                            operator
                                            filters {
                                            field
                                            condition
                                            values
                                            negated
                                                }
                                            }
                                        }
                                    }
                                }
      """
            variables = {
                "urn": current_views[view_name],
                "input": {
                    "name": view_name,
                    "description": view_info["description"],
                    "definition": view_info["definition"],
                },
            }

            result, err = run_graphql(
                graph,
                graphql_query=query,
                variables=variables,
            )

            if err:
                context.log.error(err)

        else:
            context.log.info(f"emitting view {view_name}")

            result, err = run_graphql(
                graph,
                graphql_query="""mutation createView($input: CreateViewInput!) {
                            createView(input: $input) {
                                urn
                                type
                                viewType
                                name
                                description
                                definition {
                                entityTypes
                                filter {
                                    operator
                                    filters {
                                    field
                                    condition
                                    values
                                    negated
                                    }
                                }
                                }
                            }
                            }

                                      """,
                variables={
                    "input": {
                        "viewType": "GLOBAL",
                        "name": view_name,
                        "description": view_info["description"],
                        "definition": view_info["definition"],
                    },
                },
            )

            if err:
                context.log.info(err)

    views_to_remove = {x: y for x, y in current_views.items() if x not in datahub_views}

    context.log.info(f"views_to_remove: {views_to_remove}")

    for view_urn in views_to_remove.values():
        result, err = run_graphql(
            graph,
            graphql_query="""mutation deleteView($urn: String!) {
                                deleteView(urn: $urn)
                            }
                        """,
            variables={"urn": view_urn},
        )

        if err:
            context.log.error(err)
    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_non_dagster_urls_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)

    table_sources_from_s3_records = get_s3_objects("table_sources")
    table_sources_from_s3_records_dict = dict(table_sources_from_s3_records)

    assert len(table_sources_from_s3_records_dict) == len(
        table_sources_from_s3_records
    ), "duplicate table sources"

    urns = get_all_urns(context)

    source_code_scrapes = run_dbx_query(
        f"""
        select table, file_location from auditlog.dim_table_source_code_scrapes
        """
    )

    source_code_scrapes_dict = {x[0]: x[1] for x in source_code_scrapes}

    for dataset_urn in urns:
        dataset_urn = dataset_urn["entity"]["urn"]

        catalog, database, table = get_db_and_table(dataset_urn)

        if database_filter and database_filter not in database:
            continue

        github_url_from_scrape = None

        if f"{database}.{table}" in source_code_scrapes_dict:
            github_url_from_scrape = source_code_scrapes_dict[f"{database}.{table}"]

        if (
            database.startswith("kinesisstats")
            or database in rds_dbs
            or database in non_domain_dbs
            or database in datastreams_dbs
            or database in datapipelines_dbs
            or database in biztech_dbs
            or database.startswith("mixpanel")
            or database == "dynamodb"
            or database == "definitions"
            or database == "s3bigstats"
            or database == "metrics_repo"
        ):
            # DBX job urls - KS, RDS and Dynamo only
            if database.startswith("kinesisstats"):
                dbx_job_url = f"https://samsara-dev-us-west-2.cloud.databricks.com/jobs?o=5924096274798303&query={table}"
            elif database in rds_dbs:
                updated_table = table.replace("_v0", "")
                dbx_job_url = f"https://samsara-dev-us-west-2.cloud.databricks.com/jobs?o=5924096274798303&query={updated_table}"
            elif database == "dynamodb":
                updated_table = table.replace("_", "-")
                dbx_job_url = f"https://samsara-dev-us-west-2.cloud.databricks.com/jobs?o=5924096274798303&query=dynamodb-merge-{updated_table}"

            links_to_add = {
                "Databricks URL": f"https://samsara-dev-us-west-2.cloud.databricks.com/explore/data/default/{database}/{table}?o=5924096274798303#",
            }

            if database in ["kinesisstats", *rds_dbs, "dynamodb"]:
                links_to_add["Databricks Job URL"] = dbx_job_url

            if table_sources_from_s3_records_dict.get(f"{database}.{table}"):
                if database in ["kinesisstats"]:
                    if get_datahub_env() == "prod":
                        file_path = "/dataplatform/ksdeltalake/registry.go"
                    else:
                        file_path = f"{os.environ.get('BACKEND_ROOT')}/go/src/samsaradev.io/infra/dataplatform/ksdeltalake/registry.go"

                    line_number = find_line_number_in_code(
                        file_path, f"objectstatproto.ObjectStatEnum_{table}"
                    )

                    if not line_number:
                        context.log.error(
                            f"Unable to find line number for {table}, so returning line number 1 for file"
                        )
                        line_number = 1

                    github_url = f"https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/ksdeltalake/registry.go#L{line_number}"

                    links_to_add["GitHub URL"] = github_url
                else:
                    links_to_add["GitHub URL"] = table_sources_from_s3_records_dict[
                        f"{database}.{table}"
                    ]

            links_to_add["Submit Feedback"] = f"{GOOGLE_FORMS_URL_PREFIX}{dataset_urn}"

            if github_url_from_scrape:
                links_to_add[
                    "GitHub URL"
                ] = f"https://github.com/samsara-dev/backend/blob/master/{github_url_from_scrape}"

            add_links(context, graph, dataset_urn, links_to_add)

    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_keys_to_datahub(context) -> int:
    op_start_time = time.time()
    asset_specs_by_key = get_asset_specs_by_key()
    graph = get_graph(context)
    emitter = get_emitter()
    urns = get_all_urns(context)

    primary_keys_from_s3_records = get_s3_objects("table_unique_keys")

    if EMIT_EMBEDDED_LINKS:

        PREVIEW_DB_ALLOWLIST = [
            "apptelematics",
            "billing",
            "clouddb",
            "coachingdb_shards",
            "customer360",
            "data_analytics",
            "dataengineering",
            "datamodel_core",
            "datamodel_core_bronze",
            "datamodel_core_silver",
            "datamodel_platform_bronze",
            "datamodel_platform_silver",
            "datamodel_platform",
            "datamodel_safety_bronze",
            "datamodel_safety_silver",
            "datamodel_safety",
            "datamodel_telematics_bronze",
            "datamodel_telematics_silver",
            "datamodel_telematics",
            "dataprep",
            "dataprep_safety",
            "dataprep_telematics",
            "datastreams",
            "definitions",
            "feature_store",
            "inference_store",
            "firmware",
            "firmwaredb",
            "firmware_dev",
            "hardware",
            "kinesisstats",
            "kinesisstats_window",
            "perf_infra",
            "productsdb",
            "product_analytics",
            "product_analytics_staging",
            "safetydb_shards",
            "safetyeventreviewdb",
            "samsara_zendesk",
            "s3bigstats",
        ]

        PREVIEW_CATALOG_ALLOWLIST = [
            "default",
        ]

        for urn in urns:  # just for a few testing urns at first when run
            urn = urn["entity"]["urn"]

            catalog, db, table = get_db_and_table(urn)

            if db not in PREVIEW_DB_ALLOWLIST:
                continue

            if catalog not in PREVIEW_CATALOG_ALLOWLIST:
                continue

            try:
                json_data = get_s3_preview_data(db, table)
            except Exception as e:
                context.log.info(e)
                continue

            if db in ["s3bigstats"]:
                markdown = transpose_single_row_scrollable(json_data)
            else:
                markdown = display_multiple_rows_scrollable(json_data)

            render_url = markdown

            embed_mcp = create_embed_mcp(urn, render_url)
            emit_event(emitter, embed_mcp, event_type="mcp")

    # emit primary keys
    def _add_primary_keys_to_table(table_name, primary_keys):

        dataset_urn = builder.make_dataset_urn(platform, table_name)

        current_editable_schema_metadata = graph.get_aspect(
            entity_urn=dataset_urn,
            aspect_type=SchemaMetadataClass,
        )

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.fields:
                col_name = get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)

                if col_name in primary_keys:

                    graphql_query = f"""
                        mutation batchAddTags($input: BatchAddTagsInput!) {{
                        batchAddTags(input: $input)
                            }}
                    """

                    variables = {
                        "input": {
                            "tagUrns": ["urn:li:tag:Primary Key"],
                            "resources": [
                                {
                                    "resourceUrn": dataset_urn,
                                    "subResource": fieldInfo.fieldPath,
                                    "subResourceType": "DATASET_FIELD",
                                }
                            ],
                        }
                    }

                    result, err = run_graphql(graph, graphql_query, variables=variables)
                    if err:
                        context.log.error(err)

    tables_in_graph = [
        get_db_and_table(x["entity"]["urn"], return_type="string")
        for x in get_all_urns(context)
    ]

    for asset_key, asset_spec in asset_specs_by_key.items():
        if len(asset_key.path) != 3:
            continue
        region, database, table = tuple(asset_key.path)

        if database_filter and database_filter not in database:
            continue

        if (
            (
                database.startswith("datamodel_")
                or database.startswith("firmware_dev")
                or database
                in [
                    "auditlog",
                    "product_analytics",
                    "product_analytics_staging",
                    "dataengineering",
                    "feature_store",
                    "inference_store",
                ]
            )
            and not (database.endswith("_dev") and not database.startswith("firmware"))
            and not table.startswith("dq_")
            and region == AWSRegions.US_WEST_2.value
            and f"{database}.{table}" in tables_in_graph
        ):
            table_name = f"{database}.{table}"

            asset_context = asset_spec.metadata
            primary_keys = asset_context.get("primary_keys", [])

            _add_primary_keys_to_table(table_name, primary_keys)

    for table_name, primary_keys in primary_keys_from_s3_records:

        if database_filter and database_filter not in table_name:
            continue

        _add_primary_keys_to_table(table_name, primary_keys)

    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_partitions_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)
    urns = get_all_urns(context)

    REFRESH_ALL_PARTITIONS = False

    # refresh all partitions each friday
    if datetime.today().weekday() == 4:
        context.log.info("refreshing all partitions on Friday")
        REFRESH_ALL_PARTITIONS = True

    i = 0
    start_time = time.time()

    table_partitions_from_s3_records = get_s3_objects("table_partitions")

    # emit amundsen partitions
    for table_name, partition_lst in table_partitions_from_s3_records:
        urn = make_dataset_urn(platform, table_name)

        if database_filter and database_filter not in table_name:
            continue

        current_editable_schema_metadata = graph.get_aspect(
            entity_urn=urn,
            aspect_type=SchemaMetadataClass,
        )

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.fields:
                col_name = get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)

                if col_name in partition_lst:

                    graphql_query = f"""
                        mutation batchAddTags($input: BatchAddTagsInput!) {{
                        batchAddTags(input: $input)
                            }}
                    """

                    variables = {
                        "input": {
                            "tagUrns": ["urn:li:tag:Partition"],
                            "resources": [
                                {
                                    "resourceUrn": urn,
                                    "subResource": fieldInfo.fieldPath,
                                    "subResourceType": "DATASET_FIELD",
                                }
                            ],
                        }
                    }

                    result, err = run_graphql(graph, graphql_query, variables=variables)
                    if err:
                        context.log.error(err)

    all_partitions_in_warehouse = run_dbx_query(
        """
                select concat(table_schema, '.',  table_name) as table_name,
                column_name from system.information_schema.columns
                where partition_index is not null
            """
    )

    all_partitions_in_warehouse = pd.DataFrame(
        all_partitions_in_warehouse, columns=["table_name", "column_name"]
    )

    for urn in urns:

        i += 1

        if i % 100 == 0:
            current_time = time.time()
            elapsed_time = current_time - start_time
            context.log.debug(f"processed {i} urns")
            context.log.debug(
                f"estimated total run time: {round((elapsed_time/i)*len(urns)/60,2)} minutes"
            )

        urn = urn["entity"]["urn"]

        catalog, database, table = get_db_and_table(urn)

        if database_filter and database_filter not in database:
            continue

        # find all partitions on Saturday
        bucket_name, delta_table_prefix = get_s3_bucket_from_table(graph, urn)

        if not bucket_name:
            continue

        if os.getenv("MODE") in (
            "LOCAL_PROD_CLUSTER_RUN",
            "DRY_RUN",
            "LOCAL_DATAHUB_RUN",
        ):
            s3 = boto3.client("s3")
        else:
            s3 = get_datahub_eks_s3_client()

        current_global_tags = graph.get_aspect(
            entity_urn=urn, aspect_type=GlobalTagsClass
        )

        # initially only roll out partition logging for datamodel tables
        if REFRESH_ALL_PARTITIONS and (
            database.startswith("datamodel_")
            or database
            in [
                "product_analytics",
                "product_analytics_staging",
                "dataengineering",
                "feature_store",
                "inference_store",
            ]
        ):
            # only refresh all partitions for partitioned tables
            if not current_global_tags:
                continue
            elif "urn:li:tag:Partitioned" not in [
                x.get("tag") for x in current_global_tags.tags
            ]:
                continue

            all_partitions = get_all_partitions_from_s3(
                context,
                s3,
                bucket_name,
                delta_table_prefix,
            )

            raw_json = json.dumps(all_partitions)

            s3_client = boto3.client("s3")

            s3_client.put_object(
                Body=raw_json,
                Bucket="samsara-amundsen-metadata",
                Key=f"staging/landed_partitions/{get_datahub_env()}/us-west-2/{database}/{table}.json",
            )

        if (
            not REFRESH_ALL_PARTITIONS
            and current_global_tags is not None
            and hasattr(current_global_tags, "tags")
            and bool(
                set(["urn:li:tag:Partitioned", "urn:li:tag:Unpartitioned"])
                & set([x.get("tag") for x in current_global_tags.tags])
            )
        ):
            # A table's partitions cannot be changed, so we only need to store the partitions once
            # If a table is dropped and recreated, we will have a GC job that that removes
            # dropped tables from DataHub, after which it will be re-ingested and the partition tag will be unset
            continue

        GET_PARTITIONS_FROM_WAREHOUSE = True

        # for kinesisstats, we derived the partition information from the
        # underlying s3 location, instead of describing the view, which
        # would not work
        if GET_PARTITIONS_FROM_WAREHOUSE and database not in ["kinesisstats"]:

            try:
                cols = all_partitions_in_warehouse.loc[
                    (all_partitions_in_warehouse["table_name"] == f"{database}.{table}")
                ]["column_name"].tolist()

            except Exception as e:
                context.log.error(e)
                context.log.error(f"unable to get partitions for {urn}")
                cols = []

            partitions_from_warehouse = cols
            partitions = partitions_from_warehouse
            partition_lst = partitions

        else:
            partitions = get_latest_merge_or_insert(
                context,
                s3,
                bucket_name,
                delta_table_prefix,
                value_to_find="partitions",
            )

        if partitions:
            try:
                if isinstance(partitions, str):
                    partition_lst = ast.literal_eval(partitions)
                elif isinstance(partitions, list):
                    partition_lst = partitions
                else:
                    context.log.error(
                        f"partitions is not a list or string: {partitions}"
                    )
                    partition_lst = []
                    partitions = "[]"
            except Exception as e:
                context.log.error(e)
                context.log.error(
                    f"error extracting partitions ({partitions}) for {urn}"
                )
                partition_lst = []
                partitions = "[]"
        else:
            context.log.warning(f"no partitions extracted for {urn}")
            partition_lst = []
            partitions = "[]"

        if partition_lst:
            partitioned_tag_aspect = GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:Partitioned")]
            )

            emitter = get_emitter()
            ev = MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=partitioned_tag_aspect
            )
            emit_event(emitter, ev)
        else:

            unpartitioned_tag_aspect = GlobalTagsClass(
                tags=[TagAssociationClass(tag="urn:li:tag:Unpartitioned")]
            )

            emitter = get_emitter()
            ev = MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=unpartitioned_tag_aspect
            )
            emit_event(emitter, ev)

            continue

        current_editable_schema_metadata = graph.get_aspect(
            entity_urn=urn,
            aspect_type=SchemaMetadataClass,
        )

        if current_editable_schema_metadata:
            for fieldInfo in current_editable_schema_metadata.fields:
                col_name = get_simple_field_path_from_v2_field_path(fieldInfo.fieldPath)

                if col_name in partition_lst:

                    graphql_query = f"""
                        mutation batchAddTags($input: BatchAddTagsInput!) {{
                        batchAddTags(input: $input)
                            }}
                    """

                    variables = {
                        "input": {
                            "tagUrns": ["urn:li:tag:Partition"],
                            "resources": [
                                {
                                    "resourceUrn": urn,
                                    "subResource": fieldInfo.fieldPath,
                                    "subResourceType": "DATASET_FIELD",
                                }
                            ],
                        }
                    }

                    result, err = run_graphql(graph, graphql_query, variables=variables)
                    if err:
                        context.log.error(err)

    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def create_tags_and_terms(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)

    def _add_tag_or_term(
        id,
        name,
        description,
        entity_type: Literal["GLOSSARY_TERM", "TAG"] = "GLOSSARY_TERM",
        links=None,
    ):
        context.log.info(f"adding {name} to graph with entity_type: {entity_type}")

        if entity_type == "GLOSSARY_TERM":
            query = """
                    mutation createGlossaryTerm($input: CreateGlossaryEntityInput!) {
                createGlossaryTerm(input: $input)
                }
        """
        else:
            query = """
                    mutation createTag($input: CreateTagInput!) {
                        createTag(input: $input)
                            }
            """

        variables = {
            "input": {
                "id": id,
                "name": name,
                "description": description,
            }
        }

        result, err = run_graphql(graph, graphql_query=query, variables=variables)

        if err:
            context.log.error(err)
            result2, err2 = run_graphql(
                graph,
                graphql_query="""
                            mutation updateDescription($input: DescriptionUpdateInput!) {
                                        updateDescription(input: $input)
                        }
                """,
                variables={
                    "input": {
                        "description": description,
                        "resourceUrn": f"urn:li:{'glossaryTerm' if entity_type == 'GLOSSARY_TERM' else 'tag'}:{id}",
                    }
                },
            )
            if err2:
                context.log.error(err2)

        if links and entity_type == "GLOSSARY_TERM":
            add_links(context, graph, f"urn:li:glossaryTerm:{id}", links)

    glossary_terms = [
        {
            "id": "1dd7fa08-22a3-4e10-95d3-b671b0100439",
            "name": "Certified",
            "description": 'Beyond bronze/silver/gold layer designations, we have established an additional category called "Certified" which indicates additional scrutiny has been placed on this asset, and it can be trusted for building downstream analytics. These badges are manually applied and reviewed monthly for adherence to best practices.\n\nCurrent Certification Criteria (updated 7/24/2024)\n\n*   High quality table description (via table documentation helper)\n    \n*   High quality column descriptions (90%+ filled)\n    \n*   At least 3 Data Quality checks (including checks for primary keys and empty partitions)\n    \n*   Published technical owning team\n    \n*   Pipeline monitored for failures\n    \n*   Published SLOs\n    \n*   Must have data contract\n    \n*   Must have example highlighted queries',
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17860390/Data+Model+Table+Certification+Criteria",
                "Dashboard": "https://samsara-dev-us-west-2.cloud.databricks.com/dashboardsv3/01ef7c3587aa190590ef8d20fad210aa/published?o=5924096274798303",
            },
        },
        {
            "id": "89273152-70de-4a4e-9e2b-eba1261f45c6",
            "name": "Unverified",
            "description": "\\[EXPERIMENTAL\\]\n\nOur detection algorithms estimate this table was produced by an individual user and not a scheduled workflow (Dagster, Databricks, etc).\n\nThis data table is currently without ongoing maintenance or a defined Service Level Objective (SLO), which may compromise its reliability and accuracy for critical decision-making processes or downstream workloads. It is advisable to verify the data independently before using in downstream pipelines or analysis.\n\nPlease report false positives or other feedback [here](https://docs.google.com/forms/d/e/1FAIpQLSdlCB6Fn0luoQWVQYfG6xWld-ovV5Hjl3PclrlE6DAPezzSbA/viewform?usp=pp_url&entry.2018362552=urn:li:glossaryTerm:89273152-70de-4a4e-9e2b-eba1261f45c6)",
            "entity_type": "GLOSSARY_TERM",
        },
        {
            "id": "glossary_term_beta",
            "name": "Beta",
            "description": 'NOT READY FOR WIDE USE.\nWhenever a new table lands in one of our "gold" data model databases (datamodel_core, datamodel_telematics, datamodel_safety, datamodel_platform, dataengineering, or product_analytics), we apply the Beta tag to indicate that the table is still in the experimental phase and may undergo significant changes. We will remove this tag once the table has been certified, which is intended to happen within 30 days of the table landing.',
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/DATA/pages/17913159",
            },
        },
        {
            "id": "glossary_term_bvs",
            "name": "Business Value",
            "description": "Metrics that have been identified to be part of the Business Value Metrics initiative.",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/7932108/Value+Metrics+Program",
            },
        },
        {
            "id": "glossary_term_mpr",
            "name": "Monthly Product Review",
            "description": "Metrics that have been identified to be part of the Monthly Product Reviews work within the Data org.",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5881062/FY25+Q2+MPR+and+Dashboard+Automation",
            },
        },
        {
            "id": "glossary_term_safety",
            "name": "Safety",
            "description": "Metrics that are related to the Safety pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6109258/Safety+Sites+Product+Reviews",
            },
        },
        {
            "id": "glossary_term_telematics",
            "name": "Telematics",
            "description": "Metrics that are related to the Telematics pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6054861/Telematics+Product+Reviews",
            },
        },
        {
            "id": "glossary_term_ai",
            "name": "AI",
            "description": "Metrics that are related to the AI pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4671492/AI+Monthly+Product+Review+-+Proposal",
            },
        },
        {
            "id": "glossary_term_platform",
            "name": "Platform",
            "description": "Metrics that are related to the Platform pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/6027132/Platform+Product+Review",
            },
        },
        {
            "id": "glossary_term_training",
            "name": "Training",
            "description": "Metrics that are within the Training/Coaching pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4574788/Training+Metrics+Dashboard+Index",
            },
        },
        {
            "id": "glossary_term_forms",
            "name": "Forms",
            "description": "Metrics that are within the Forms/Workflows pillar",
            "entity_type": "GLOSSARY_TERM",
            "links": {
                "Glossary Term Details": "https://samsaradev.atlassian.net/wiki/spaces/~712020ca2e44d87b3245608191f8f39fd1c33a/pages/3566174209/Admin+Insights+Metrics+v1",
            },
        },
    ]

    tables_in_graph = [
        get_db_and_table(x["entity"]["urn"], return_type="string")
        for x in get_all_urns(context)
    ]

    tags = [
        {
            "id": "Partition",
            "name": "Partition",
            "description": "A partition is a subset of data within a table that is defined by a specific value or range of values for one or more columns.",
            "entity_type": "TAG",
        },
        {
            "id": "Primary Key",
            "name": "Primary Key",
            "description": "A primary key is a column or set of columns that uniquely identifies each row in a table.",
            "entity_type": "TAG",
        },
        {
            "id": "Partitioned",
            "name": "Partitioned",
            "description": "A table is partitioned if it has a column or set of columns that define subsets of data within the table.",
            "entity_type": "TAG",
        },
        {
            "id": "Unpartitioned",
            "name": "Unpartitioned",
            "description": "A table is unpartitioned if it does not have any partition columns.",
            "entity_type": "TAG",
        },
        {
            "id": "us_only",
            "name": "US Cloud Only",
            "description": "This table is only queryable from the Databricks us-west-2 Cloud Account",
            "entity_type": "TAG",
        },
        {
            "id": "us_eu_cloud",
            "name": "US + EU Clouds",
            "description": "This table is queryable from both the Databricks us-west-2 Cloud Account and the Databricks eu-west-1 Cloud Account. This means the same table name exists for each region, and you need to query both tables to get complete data",
            "entity_type": "TAG",
        },
        {
            "id": "us_eu_ca_cloud",
            "name": "US + EU + CA Clouds",
            "description": "This table is queryable from the Databricks us-west-2 Cloud Account, the Databricks eu-west-1 Cloud Account, and the Databricks ca-central-1 Cloud Account. This means the same table name exists for each region, and you need to query all three tables to get complete data",
            "entity_type": "TAG",
        },
        {
            "id": "copied_from_eu_to_us",
            "name": "Copied from EU to US Cloud",
            "description": "This table is queryable from the Databricks US Cloud Account, and contains data that was previously only queryable from the Databricks EU Cloud Account. These tables are typically created when a table in EU needs to be combined with other data to create a single unified table in US.",
            "entity_type": "TAG",
        },
        {
            "id": "combined_global_us_cloud",
            "name": "Combined Global Data in US Cloud",
            "description": "This table is queryable from the Databricks US Cloud Account, and contains aggregated data from all regions.",
            "entity_type": "TAG",
        },
    ]

    for tag in tags:
        _add_tag_or_term(*tag.values())

    for term in glossary_terms:
        _add_tag_or_term(*term.values())

    # remove unused tags
    all_tags_in_graph = [
        x["entity"]["urn"] for x in get_all_urns(context, entity_type="TAG")
    ]

    context.log.info(f"all_tags_in_graph: {all_tags_in_graph}")

    # TODO (remove unused glossary terms in future)
    tags_to_remove = [
        x
        for x in all_tags_in_graph
        if x.replace("urn:li:tag:", "") not in [y["id"] for y in tags]
        and not x.find("dbt:") >= 0
    ]

    context.log.info(f"tags_to_remove: {tags_to_remove}")

    for tag in tags_to_remove:
        context.log.info(f"removing tag: {tag}")
        sync_tags_or_terms(
            context=context,
            graph=graph,
            tag_or_term_urn=tag,
            entity_type="TAG",
            tbl_list=[],  # applying an empty list effectively removes the tag from all tables via GC routine
            tables_in_graph=tables_in_graph,
        )

        graphql_query = """
            mutation deleteTag($urn: String!) {
            deleteTag(urn: $urn)
            }
        """

        variables = {
            "urn": tag,
        }

        result, err = run_graphql(
            graph, graphql_query=graphql_query, variables=variables
        )
        if err:
            context.log.error(err)

    unverified_tables = get_untrustworthy_tables()

    context.log.info(f"unverified_tables: {unverified_tables}")

    system_table_query = "select concat(table_schema, '.', table_name) as table_name from system.information_schema.tables"

    eu_tables = run_dbx_query(
        system_table_query,
        region=AWSRegions.EU_WEST_1.value,
    )

    ca_tables = run_dbx_query(
        system_table_query,
        region=AWSRegions.CA_CENTRAL_1.value,
    )

    us_tables = run_dbx_query(
        system_table_query,
        region=AWSRegions.US_WEST_2.value,
    )

    s3 = boto3.client("s3")

    obj_data = s3.get_object(
        Bucket="samsara-datahub-metadata",
        Key="metadata/exports/prod/lineage/lineage_map.json",
    )
    lineage_map = json.loads(obj_data["Body"].read().decode("utf-8"))

    # we should update this to include all views, not just metrics
    metrics_with_global_lineage = [
        x
        for x, y in lineage_map.items()
        if x.split(".")[0] == "metrics_repo" and any(z.endswith("_global") for z in y)
    ]

    eu_tables = [row.table_name for row in eu_tables]
    ca_tables = [row.table_name for row in ca_tables if row.table_name in eu_tables]
    eu_tables = [row for row in eu_tables if row not in ca_tables]
    us_tables = [row.table_name for row in us_tables]
    global_tables = [
        x
        for x in us_tables
        if x.endswith("_global") or x in metrics_with_global_lineage
    ]
    eu_copy_tables = [x for x in us_tables if x.endswith("_eu")]
    us_only_tables = [
        x
        for x in us_tables
        if x not in global_tables
        and x not in eu_tables
        and x not in eu_copy_tables
        and x not in ca_tables
    ]

    asset_specs_by_key = get_asset_specs_by_key()

    platform_metrics = []
    ai_metrics = []
    telematic_metrics = []
    safety_metrics = []
    mpr_metrics = []
    business_value_metrics = []
    training_metrics = []
    forms_metrics = []

    ## add tags to metrics
    for asset_key, asset_spec in asset_specs_by_key.items():
        if len(asset_key.path) != 3:
            continue
        region, database, table = tuple(asset_key.path)

        if database_filter and database_filter not in database:
            continue

        if (
            (
                database.startswith("datamodel_")
                or database.startswith("firmware_dev")
                or database
                in [
                    "metrics_repo",
                    "auditlog",
                    "product_analytics",
                    "product_analytics_staging",
                    "dataengineering",
                    "feature_store",
                    "inference_store",
                ]
            )
            and region == AWSRegions.US_WEST_2.value
            and f"{database}.{table}" in tables_in_graph
        ):
            table_name = f"{database}.{table}"

            asset_context = asset_spec.metadata
            glossary_terms = asset_context.get("glossary_terms", [])
            if "Platform" in glossary_terms:
                platform_metrics.append(table_name)
            if "Telematics" in glossary_terms:
                telematic_metrics.append(table_name)
            if "AI" in glossary_terms:
                ai_metrics.append(table_name)
            if "Safety" in glossary_terms:
                safety_metrics.append(table_name)
            if "MPR" in glossary_terms:
                mpr_metrics.append(table_name)
            if "Business Value" in glossary_terms:
                business_value_metrics.append(table_name)
            if "Training" in glossary_terms:
                training_metrics.append(table_name)
            if "Forms" in glossary_terms:
                forms_metrics.append(table_name)

    # automate removal of badge from those not on this list
    for tbl_list, tag_or_term_urn, entity_type in [
        (certified_tables, "1dd7fa08-22a3-4e10-95d3-b671b0100439", "GLOSSARY_TERM"),
        (beta_tables, "glossary_term_beta", "GLOSSARY_TERM"),
        (unverified_tables, "89273152-70de-4a4e-9e2b-eba1261f45c6", "GLOSSARY_TERM"),
        (global_tables, "combined_global_us_cloud", "TAG"),
        (us_only_tables, "us_only", "TAG"),
        (eu_tables, "us_eu_cloud", "TAG"),
        (ca_tables, "us_eu_ca_cloud", "TAG"),
        (eu_copy_tables, "copied_from_eu_to_us", "TAG"),
        (business_value_metrics, "glossary_term_bvs", "GLOSSARY_TERM"),
        (safety_metrics, "glossary_term_safety", "GLOSSARY_TERM"),
        (telematic_metrics, "glossary_term_telematics", "GLOSSARY_TERM"),
        (ai_metrics, "glossary_term_ai", "GLOSSARY_TERM"),
        (mpr_metrics, "glossary_term_mpr", "GLOSSARY_TERM"),
        (platform_metrics, "glossary_term_platform", "GLOSSARY_TERM"),
        (training_metrics, "glossary_term_training", "GLOSSARY_TERM"),
        (forms_metrics, "glossary_term_forms", "GLOSSARY_TERM"),
    ]:
        sync_tags_or_terms(
            context=context,
            graph=graph,
            tag_or_term_urn=tag_or_term_urn,
            entity_type=entity_type,
            tbl_list=tbl_list,
            tables_in_graph=tables_in_graph,
        )

    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
)
def emit_table_sizes_to_datahub(context) -> int:
    op_start_time = time.time()
    graph = get_graph(context)
    emitter = get_emitter()
    urns = get_all_urns(context)
    context.log.info(f"processing {len(urns)} urns")
    i = 0

    start_time = time.time()

    tables_with_recent_updates = run_dbx_query(
        """
            select
        concat(database, '.', table) as table_name
        from
        auditlog.datahub_scrapes
        where
        date = (
            select
            max(date)
            from
            auditlog.datahub_scrapes
        )
        and last_update <> 'NaN'
        and storage_location like 's3://samsara%'
        AND DATE(FROM_UNIXTIME(last_update / 1000)) >= DATE_SUB(date, 1)
        order by
        last_update desc
        """
    )

    tables_with_recent_updates = [row.table_name for row in tables_with_recent_updates]

    urns = [
        x
        for x in urns
        if get_db_and_table(x["entity"]["urn"], return_type="string")
        in tables_with_recent_updates
    ]

    context.log.info(f"processing {len(urns)} urns with recent updates")

    for urn in urns:
        if i % 100 == 0:
            context.log.info(i)
        urn = urn["entity"]["urn"]

        s3_location = ""
        i += 1

        if i % 100 == 0:
            context.log.info(f"processing {i} of {len(urns)}")
            current_time = time.time()
            elapsed_time = current_time - start_time
            context.log.debug(f"processed {i} urns")
            context.log.debug(
                f"estimated total run time: {round((elapsed_time/i)*len(urns)/60,2)} minutes"
            )

        catalog, database, table = get_db_and_table(urn)

        if database_filter and database_filter not in database:
            continue

        bucket_name, delta_table_prefix = get_s3_bucket_from_table(graph, urn)

        if not bucket_name:
            continue

        if os.getenv("MODE") in (
            "LOCAL_PROD_CLUSTER_RUN",
            "DRY_RUN",
            "LOCAL_DATAHUB_RUN",
        ):
            s3 = boto3.client("s3")
        else:
            s3 = get_datahub_eks_s3_client()

        try:
            table_size, num_files = get_file_size_from_crc(
                context, s3, bucket_name, delta_table_prefix
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucket":
                context.log.error(
                    f"The specified bucket ({bucket_name}) does not exist."
                )
            else:
                context.log.error(
                    f"Error: {e} for bucket {bucket_name} and prefix {delta_table_prefix}"
                )
            continue

        event = MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=DatasetProfileClass(
                timestampMillis=int(time.time() * 1000), sizeInBytes=table_size
            ),
        )

        err = emit_event(emitter, event)

        if err:
            context.log.error(err)

        # emit num_files as structured property
        if num_files:
            emit_structured_properties(graph, urn, {"num_files": num_files})

    log_op_duration(op_start_time)
    return 0


class QueryStatsBackfillConfig(Config):
    backfill_days: int = 1


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
)
def extract_query_stats_to_datahub(context, config: QueryStatsBackfillConfig) -> int:
    op_start_time = time.time()

    if os.getenv("MODE") == "DRY_RUN":
        context.log.info("DRY_RUN")
        return 0

    if get_datahub_env() == "prod":
        os.environ["DATAHUB_GMS_TOKEN"] = get_datahub_token()
        os.environ["DATAHUB_GMS_URL"] = get_gms_server()
    else:
        os.environ["DATAHUB_GMS_URL"] = "http://localhost:8080"

    if get_datahub_env() == "prod":
        path_to_recipe = "/datamodel/datamodel/resources/datahub"
    else:
        backend_root = os.getenv("BACKEND_ROOT")
        path_to_recipe = f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/resources/datahub"

    yaml_file = f"{path_to_recipe}/sql_queries_recipe.yaml"

    def get_query(date_str):
        return """
                select
user_identity.email as user,
request_params.commandText as query,
    unix_timestamp(event_time) as timestamp
from
system.access.audit
where
event_date  = '{date_str}'
and request_params.commandText is not null
and lower(request_params.commandText) like '%select%'
and lower(request_params.commandText) not like '%create%'
and lower(request_params.commandText) not like '%describe%'
and request_params.commandText not like '%system generated query%'
and service_name in ('databrickssql', 'notebook')
and user_identity.email like '%@samsara.com'
and user_identity.email not like '%databricks%'
and lower(request_params.commandText) not like '%spark.sql%'
and lower(request_params.commandText) not like '%dbutils.widgets%'
and lower(request_params.commandText) not like '%import pandas%'
and lower(request_params.commandText) not like '%spark.table(%'
and lower(request_params.commandText) not like '%import seaborn%'
and lower(request_params.commandText) not like '%import boto3%'
and lower(request_params.commandText) not like '%from pyspark.%'
and lower(request_params.commandText) not like '%from sklearn.%'
and lower(request_params.commandText) not like '%df[%'
and lower(request_params.commandText) not like '%(truncated)%'
and lower(request_params.commandText) not like '%.toPandas%'
and lower(request_params.commandText) not like '%.select(%'
and lower(request_params.commandText) not like '%f""%'
and lower(request_params.commandText) not like '%${{%'
and lower(request_params.commandText) not like '%sparkr::sql%'
and lower(request_params.commandText) not like '%merge into%'
and lower(request_params.commandText) not like '%--endsql%'
and not (lower(request_params.commandText) like '%explode(results) as (`key`, value)%' and lower(request_params.commandText) like '%auditlog.dq_check_log%')
and (response.status_code != 1 or response.status_code is null)
and workspace_id in (5924096274798303, 7702501906276199)
        """.format(
            date_str=date_str
        )

    # loop through the last n days
    for i in range(config.backfill_days, 0, -1):
        date_str = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")

        query = get_query(date_str)

        # workspace_id 5924096274798303 is the US Databricks workspace (does not include EU queries)
        # temp job to copy data from POC workspace: https://samsara-biztech-poc.cloud.databricks.com/?o=5058836858322111#job/434002352770055

        result = run_dbx_query(query)

        df = pd.DataFrame(result, columns=["user", "query", "timestamp"])

        context.log.info(len(df))

        df_dict = df.to_dict(orient="records")

        # if sql_queries.json exists, delete it
        if os.path.exists("sql_queries.json"):
            os.remove("sql_queries.json")

        with open("sql_queries.json", "w") as file:
            for record in df_dict:
                json_str = json.dumps(record)
                file.write(json_str + "\n")

        result = subprocess.getoutput(f"/datahub_env/bin/datahub ingest -c {yaml_file}")
        if "Pipeline finished successfully; produced" not in result:
            context.log.error(result)
            raise Exception("Error running datahub ingest")
        else:
            context.log.info(result)

    log_op_duration(op_start_time)
    return 0


def _metric_queries_parameters(dimensions: List[str]) -> List[OrderedDict[str, str]]:
    queries_parameters = []
    parameter_items = []

    # Set default parameter values for dimensions
    for dim in dimensions:
        if dim.get("name") == "date":
            parameter_items.append(
                (dim.get("name"), "ARRAY('2024-01-01', '2024-01-02', 'DAILY')")
            )
        else:
            parameter_items.append((dim.get("name"), "ARRAY()"))

    # Save parameters for example query #1
    queries_parameters.append(
        OrderedDict(
            filter(lambda x: x[0] == "date" or x[0] == "org_id", parameter_items)
        )
    )
    # Save parameters for example query #2
    for idx, parameter_item in enumerate(parameter_items):
        if parameter_item[0] == "org_id":
            parameter_items[idx] = ("org_id", "ARRAY(7002691, 60967, 'COLLAPSED')")
    queries_parameters.append(OrderedDict(parameter_items))

    queries_parameters.reverse()
    return queries_parameters


def _metric_query_title(
    source: str, parameters_by_dimension: OrderedDict[str, str]
) -> str:
    title = source.split(".")[-1].replace("_", " ").capitalize()
    for dim, param in parameters_by_dimension.items():
        param_value = re.findall(r"array\(([a-zA-Z0-9,_\-\' ]+)\)", param.lower())
        # 'date' is a special input argument that we express differently than other dimensions
        if dim == "date":
            title += f" between {' and '.join([val.strip() for val in param_value[0].split(',')])}"

        else:
            if param.lower() == "null":
                title += f" for all '{dim}'"
            elif param.lower() == "array()":
                title += f" by '{dim}'"
            elif param.lower().startswith("array("):
                title += f" by '{dim}' ({param_value[0]})"
    return title


def _metric_query_statement(
    source: str, parameters_by_dimension: OrderedDict[str, str]
) -> str:
    statement = "SELECT {projections} FROM {source}({params})"
    projections = []
    params = []
    for dim, value in parameters_by_dimension.items():
        if dim == "date":
            param = "date_range"
        else:
            param = dim

        if value.lower() == "null":
            params.append(f"{param} => NULL")
        elif value.lower() == "array()":
            projections.append(f"{dim}")
            params.append(f"{param} => ARRAY()")
        else:
            if "collapsed" not in value.lower():
                projections.append(f"{dim}")
            params.append(f"{param} => {value}")
    params = ", ".join(params)
    projections = ", ".join(projections + ["value"])
    return statement.format(source=source, projections=projections, params=params)


def _generate_metric_queries() -> Dict[str, OrderedDict[str, str]]:
    from dataweb._core.loaders import load_assets_defs_by_key
    from dataweb.assets import metrics

    metric_assets = load_assets_defs_by_key(metrics)

    queries = {}
    for _, asset_def in metric_assets.items():
        metric_ref = f"metrics_repo.{asset_def.key.path[-1]}"
        dimensions = asset_def.metadata_by_key[asset_def.key]["dimensions"]
        queries[metric_ref] = OrderedDict()
        queries_parameters = _metric_queries_parameters(dimensions)

        for idx, query_params in enumerate(queries_parameters):
            title = f"{len(queries_parameters)-idx}) {_metric_query_title(metric_ref, query_params)}"
            query = _metric_query_statement(
                metric_ref.replace("metrics_repo", "metrics_api"),
                query_params,
            )
            queries[metric_ref][title] = query

    return queries


@op(
    ins={"start": In(Nothing)},
    out=Out(io_manager_key="in_memory_io_manager"),
    required_resource_keys=None,
    retry_policy=None,
)
def emit_highlighted_queries_to_datahub(context) -> int:
    op_start_time = time.time()

    graph = get_graph(context)

    highlighted_query_directory = f"{os.getenv('BACKEND_ROOT')}/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/ops/datahub/highlighted_queries/queries"
    if get_datahub_env() == "prod":
        highlighted_query_directory = (
            "/datamodel/datamodel/ops/datahub/highlighted_queries/queries"
        )

    highlighted_query_map = load_highlighted_queries(highlighted_query_directory)

    metric_queries = _generate_metric_queries()
    all_queries = dict(highlighted_query_map, **metric_queries)

    context.log.info(f"highlighted_query_map: {highlighted_query_map}")

    for table, queries in all_queries.items():
        for query_name, sql_query in queries.items():

            urn = make_dataset_urn(
                platform="databricks",
                name=table,
                env="PROD",
            )

            current_queries, err = run_graphql(
                graph,
                graphql_query=f"""
                            query listQueries($input: ListQueriesInput!) {{
                                        listQueries(input: $input) {{
                                            queries {{
                                            ...query
                                            }}
                                        }}
                                        }}

                                        fragment query on QueryEntity {{
                                        urn
                                        properties {{
                                            description
                                            name
                                            statement {{
                                            value
                                            }}
                                        }}

                                        }}
                                                    """,
                variables={
                    "input": {
                        "datasetUrn": urn,
                        "start": 0,
                        "count": 1000,
                    }
                },
            )

            if err:
                context.log.error(err)

            context.log.info(f"current_queries: {current_queries}")

            current_sql_query_names = {
                x["properties"]["name"]: x["urn"]
                for x in current_queries["listQueries"]["queries"]
            }

            context.log.info(f"current_sql_query_names: {current_sql_query_names}")

            if query_name in current_sql_query_names:
                context.log.info(f"query {query_name} already exists")
                oper = "updateQuery"
                capitalized_oper = oper[0].upper() + oper[1:]
                query_id = current_sql_query_names[query_name]
                input_str_1 = f"$urn: String!, $input: {capitalized_oper}Input!"
                input_str_2 = "urn: $urn, input: $input"
            else:
                oper = "createQuery"
                capitalized_oper = oper[0].upper() + oper[1:]
                input_str_1 = f"$input: {capitalized_oper}Input!"
                input_str_2 = "input: $input"

            graphql_query = f"""mutation {oper}({input_str_1}) {{
                            {oper}({input_str_2}) {{
                                ...query
                            }}
                            }}

                            fragment query on QueryEntity {{
                            urn
                            properties {{
                                description
                                name
                                source
                                statement {{
                                value
                                language
                                }}
                                created {{
                                time
                                actor
                                }}
                                lastModified {{
                                time
                                actor
                                }}
                            }}
                            subjects {{
                                dataset {{
                                urn
                                name
                                }}
                            }}
                            }}
                                """

            variables = {
                "input": {
                    "properties": {
                        "name": query_name,
                        "description": "[Open Query Editor](https://samsara-dev-us-west-2.cloud.databricks.com/sql/editor/new?o=5924096274798303)",
                        "statement": {
                            "value": format_sql(sql_query),
                            "language": "SQL",
                        },
                    },
                    "subjects": [{"datasetUrn": urn}],
                }
            }

            if oper == "updateQuery":
                variables["urn"] = query_id

            result, err = run_graphql(graph, graphql_query, variables=variables)
            if err:
                context.log.error(err)

            # find queries no longer defined and delete them
            defined_query_names = [
                list(x.keys())[0] for x in list(all_queries.values()) if x
            ]

            context.log.info(f"defined_query_names: {defined_query_names}")

            for query in current_queries["listQueries"]["queries"]:
                if query["properties"]["name"] not in defined_query_names:
                    context.log.info(f"deleting query {query['urn']}")
                    graphql_query = """
                    mutation deleteQuery($urn: String!) {
                        deleteQuery(urn: $urn)
                    }
                    """
                    res, err = run_graphql(
                        graph,
                        graphql_query=graphql_query,
                        variables={"urn": query["urn"]},
                    )
                    if err:
                        context.log.error(err)

    log_op_duration(op_start_time)
    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    retry_policy=None,
)
def scrape_source_files(context) -> int:

    backend_root = os.getenv("BACKEND_ROOT", "")

    source_location_record = namedtuple(
        "source_location_record",
        ["file_location", "table_production_type", "has_jobs", "slack_token_count"],
    )

    # find all tables defined in workflows

    workflows_dir = f"{backend_root}/dataplatform/workflows"
    notebooks_dir = f"{backend_root}/dataplatform/notebooks"
    source_location_map: Dict[str, source_location_record] = {}
    workflow_table_list = []

    context.log.info(f"workflows_dir: {workflows_dir}")

    for root, _dirs, files in os.walk(workflows_dir):
        for file in files:
            if not file.endswith(".metadata.json") and file.endswith(".json"):
                with open(os.path.join(root, file), "r") as f:
                    parts = root.split(os.sep)
                    database = parts[parts.index("workflows") + 1]
                    data = json.load(f)
                    tasks = data.get("tasks", [])
                    tables = [
                        x.get("notebook_task", {})
                        .get("notebook_path", "")
                        .replace("/Workspace/backend/", "")
                        .replace("/backend/", "")
                        for x in tasks
                    ]
                    workflow_table_list.extend([x for x in tables if x])

    context.log.info(f"workflow_table_list: {workflow_table_list}")

    for root, _dirs, files in os.walk(notebooks_dir):
        for file in files:
            if file.endswith(".sql") or file.endswith(".py"):
                parts = root.split(os.sep)

                notebook_path = (
                    "/".join(parts[parts.index("notebooks") + 1 :]) + "/" + file
                )

                notebook_path = notebook_path.replace(".sql", "").replace(".py", "")

                # find database from metadata file
                metadata_file = os.path.join(
                    root, file.replace(".sql", "").replace(".py", "") + ".metadata.json"
                )
                if os.path.exists(metadata_file):
                    data = json.load(open(metadata_file))
                    database = data.get("output", {}).get("dbname", "")
                    table_name = file.replace(".sql", "").replace(".py", "")

                    jobs = data.get("jobs", [])
                    if len(jobs) > 0:
                        has_jobs = True
                    else:
                        has_jobs = False

                    # parse SQL file to get output database and table name
                    from sqllineage.runner import LineageRunner

                    text_strs_to_replace = {
                        "'": "`",
                        "--sql": "",
                        "--endsql": "",
                        "-- COMMAND ----------": "",
                        "-- Databricks notebook source": "",
                        "# COMMAND ----------": "",
                    }

                    try:

                        with open(os.path.join(root, file), "r") as f:
                            code_to_parse = f.read()

                            for text_str in text_strs_to_replace:
                                code_to_parse = code_to_parse.replace(text_str, "")

                            # find how many times "slack" appears in the code
                            slack_token_count = code_to_parse.lower().count(
                                "slack"
                            ) + code_to_parse.count("execute_alert(")

                        if file.endswith(".py"):
                            python_to_parse = code_to_parse

                            spark_write_patterns = [
                                r"\.write[\s\S]+?\.saveAsTable\(\s*\"([^\"]+)\"\s*\)",
                                r"\.write[\s\S]+?\.insertInto\(\s*\"([^\"]+)\"\s*\)",
                                r"DeltaTable\.forName\(spark,\s*\"([^\"]+)\"\)",
                                r"create_or_update_table\([\s\S]*?\"([^\"]+)\"",
                            ]

                            tables = []

                            for pattern in spark_write_patterns:
                                matches = re.findall(
                                    pattern,
                                    python_to_parse,
                                    re.MULTILINE | re.DOTALL,
                                )
                                for match in matches:
                                    tables.append(match)

                            if len(tables) > 0 and "." in tables[-1]:
                                database = tables[-1].split(".")[0]
                                table_name = tables[-1].split(".")[1]

                        elif file.endswith(".sql"):

                            sql_to_parse = code_to_parse

                            for text_str in text_strs_to_replace:
                                sql_to_parse = sql_to_parse.replace(text_str, "")

                            sql_lines = sql_to_parse.split("\n")
                            clean_sql_lines = [
                                line
                                for line in sql_lines
                                if not line.strip().startswith("--")
                            ]
                            sql_to_parse = "\n".join(clean_sql_lines)

                            lineage_result = LineageRunner(sql_to_parse)
                            result_deps = [
                                x.__str__()
                                for x in lineage_result.target_tables
                                if "<default>." not in x.__str__()
                            ]

                            if (
                                len(result_deps) > 0
                                and len(result_deps[-1].split(".")) == 2
                            ):
                                database = result_deps[-1].split(".")[0]
                                table_name = result_deps[-1].split(".")[1]
                            else:
                                raise Exception("no tables found via lineage parser")

                    except Exception as e:
                        print(f"error parsing SQL file: {e}")

                        # try to figure out target table from "merge into" or "insert into" or "insert overwrite" commands at end of file
                        found_tables = []
                        with open(os.path.join(root, file), "r") as f:
                            for line in f:
                                if (
                                    "merge into" in line.lower()
                                    or "insert into" in line.lower()
                                    or "insert overwrite table" in line.lower()
                                    or "insert overwrite" in line.lower()
                                    or "create or replace table" in line.lower()
                                    or "set vdp.output =" in line.lower()
                                ):
                                    if len(line.split(".")) > 1:
                                        line = (
                                            line.lower()
                                            .replace("merge into", "")
                                            .replace("insert into", "")
                                            .replace("insert overwrite table", "")
                                            .replace("insert overwrite", "")
                                            .replace("create or replace table", "")
                                            .replace("set vdp.output =", "")
                                        )
                                        database, table_name = (
                                            line.split(".")[0].strip(),
                                            line.split(".")[1].strip(),
                                        )
                                        found_tables.append(f"{database}.{table_name}")

                        if len(found_tables) > 0:
                            found_token = found_tables[-1]

                            found_token = [
                                x for x in found_token.split(" ") if "." in x
                            ]

                            if len(found_token) > 0:
                                found_token = found_token[0]

                            database = found_token.split(".")[0]
                            table_name = found_token.split(".")[1]

                    if not database:
                        # open file and try to figure out database from the first line
                        with open(os.path.join(root, file), "r") as f:
                            # split all tokens and look for DB_NAME.table_name
                            tokens = f.readline().split()

                            for token in tokens:
                                if "." in token and table_name in token:
                                    database, _ = token.split(".")
                                    break

                    if not database:
                        database = parts[parts.index("notebooks") + 1]

                file_location = os.path.relpath(os.path.join(root, file), backend_root)

                manually_specified_map = {
                    "dataplatform/notebooks/dataprep/safety/dataprep_cm_linked_vgs.sql": "dataprep_safety.cm_linked_vgs",
                    "dataplatform/notebooks/datascience/harsh_events/labeling_sample_gen/daily_k_means_for_sampling.py": "datascience.backend_k_means_cluster_for_sampling_items_to_request",
                    "dataplatform/notebooks/datascience/harsh_events/labeling_sample_gen/daily_video_request_gen.py": "datascience.daily_video_requests",
                    "dataplatform/notebooks/datascience/harsh_events/labeling_sample_gen/daily_latent_with_physics_backup_events_for_sampling.py": "datascience.backend_backup_events_items_to_request",
                    "dataplatform/notebooks/datascience/harsh_events/latent_gen/daily_backend_crash_model_latents.py": "datascience.backend_crash_model_latents",
                    "dataplatform/notebooks/datascience/harsh_events/labeling_sample_gen/daily_low_sev_crash_sampling.py": "datascience.low_sev_crash_samples_to_request",
                    "dataplatform/notebooks/datascience/harsh_events/latent_gen/daily_backend_crash_model_traces.py": "datascience.backend_crash_model_traces",
                    "dataplatform/notebooks/firmware/vdp/etl_v2/dimensions/derived_dimensions.sql": "firmware_dev.etl_immobilizer_connected",
                    "dataplatform/notebooks/firmware/vdp/etl_v2/device_second_order/second_order.sql": "firmware_dev.etl_device_second_order_statistics",
                    "dataplatform/notebooks/firmware/vdp/etl_v2/population_second_order/second_order.sql": "firmware_dev.etl_population_second_order_statistics",
                    "dataplatform/notebooks/dataplatform/dataplatform_costs.py": "billing.dataplatform_costs",
                }

                if file_location in manually_specified_map:
                    table_name = manually_specified_map.get(file_location)
                else:
                    table_name = f"{database}.{table_name}"

                source_location_map[table_name] = source_location_record(
                    file_location,
                    (
                        "workflows"
                        if notebook_path in workflow_table_list
                        else "notebooks"
                    ),
                    has_jobs,
                    slack_token_count,
                )

    context.log.info(source_location_map)

    context.log.info(
        f"total workflow produced tables= {len([y for y in source_location_map.values() if y.table_production_type=='workflows'])}"
    )
    context.log.info(
        f"total notebook produced tables= {len([y for y in source_location_map.values() if y.table_production_type=='notebooks'])}"
    )

    # export to S3

    s3_client = boto3.client("s3")

    s3_client.put_object(
        Body=json.dumps(source_location_map),
        Bucket="samsara-datahub-metadata",
        Key=f"metadata/exports/{get_datahub_env()}/source_code/source_location_map.json",
    )

    obj_data_flattened = [
        (x, y, z, z2, z3) for (x, (y, z, z2, z3)) in source_location_map.items()
    ]

    DATABASE = "auditlog" if get_datahub_env() == "prod" else "datamodel_dev"

    dim_table_source_code_scrapes_schema = {
        "table": "STRING",
        "file_location": "STRING",
        "table_production_type": "STRING",
        "has_jobs": "BOOLEAN",
        "slack_token_count": "INT",
    }

    def _recreate_table(drop_table_first=True):
        if drop_table_first:
            run_dbx_query(
                f"DROP TABLE IF EXISTS {DATABASE}.dim_table_source_code_scrapes"
            )
            time.sleep(30)

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.dim_table_source_code_scrapes (
                {", ".join([f"{k} {v}" for k, v in dim_table_source_code_scrapes_schema.items()])}
            )
            """

        run_dbx_query(create_table_query)

    # insert into auditlog.dim_table_source_code_scrapes - and use to populate burndown report for UC migration
    values = ", ".join(
        [
            f"('{table.replace('-na','').replace('-eu','')}', '{file_location}', '{table_production_type}', {has_jobs}, {slack_token_count})"
            for table, file_location, table_production_type, has_jobs, slack_token_count in obj_data_flattened
        ]
    )

    insert_query = f"""
                INSERT OVERWRITE TABLE {DATABASE}.dim_table_source_code_scrapes ({",".join(dim_table_source_code_scrapes_schema.keys())})
                VALUES {values}
                """

    try:
        run_dbx_query(insert_query)
    except Exception as e:
        context.log.error(f"error inserting into table: {e}")
        context.log.warning(
            "retrying after dropping and recreating table with updated schema"
        )
        try:
            _recreate_table(drop_table_first=True)
            run_dbx_query(insert_query)

        except Exception as e:
            context.log.error(f"error inserting into table after recreating: {e}")

    # verify table exists
    try:
        rows_in_table = run_dbx_query(
            f"SELECT COUNT(*) as cnt FROM {DATABASE}.dim_table_source_code_scrapes"
        )
        context.log.info(
            f"rows in {DATABASE}.dim_table_source_code_scrapes: {rows_in_table[0].cnt}"
        )
    except Exception as e:
        context.log.error(f"error verifying table: {e}")
        try:
            _recreate_table(drop_table_first=False)
            run_dbx_query(insert_query)
        except Exception as e:
            context.log.error(f"error recreating and inserting into table: {e}")

    return 0


@op(
    ins={"start": In(Nothing)},
    out=Out(dagster_type=int, io_manager_key="in_memory_io_manager"),
    retry_policy=None,
)
def scrape_datahub(context) -> int:
    op_start_time = time.time()

    if os.getenv("MODE") == "DRY_RUN":
        context.log.info("DRY_RUN")
        return 0

    graph = get_graph(context)

    # check for valid GMS API tokens and send Datadog metric on TTL
    tokens_query = """
        query listAccessTokens($input: ListAccessTokenInput!) {
            listAccessTokens(input: $input) {
                tokens {
                type
                name
                ownerUrn
                expiresAt
                }
            }
            }
    """

    tokens, err = run_graphql(
        graph, tokens_query, {"input": {"start": 0, "count": 100}}
    )

    if err:
        context.log.error(err)

    tokens = tokens.get("listAccessTokens", {}).get("tokens", [])

    prod_tokens = [x for x in tokens if x["type"] == "ACCESS_TOKEN"]

    if prod_tokens:
        max_expiration_time = int(max([x.get("expiresAt") for x in prod_tokens]) / 1000)
        now = int(datetime.now(timezone.utc).timestamp())
        token_ttl_hours = (max_expiration_time - now) / 3600
    else:
        token_ttl_hours = 0

    TOKEN_EXPIRATION_METRIC: str = f"{METRIC_PREFIX}.gms_token.ttl_hours"

    send_datadog_gauge_metric(TOKEN_EXPIRATION_METRIC, token_ttl_hours, [])

    df = pd.DataFrame(
        {
            "urn": pd.Series(dtype="str"),
            "database": pd.Series(dtype="str"),
            "table": pd.Series(dtype="str"),
            "poll_date_hour": pd.Series(dtype="datetime64[ns]"),
            "has_schema": pd.Series(dtype="bool"),
            "storage_location": pd.Series(dtype="str"),
            "domain": pd.Series(dtype="str"),
            "last_update": pd.Series(dtype="datetime64[ns]"),
            "table_type": pd.Series(dtype="str"),
            "date": pd.Series(dtype="str"),
            "num_assertions": pd.Series(dtype="int"),
            "num_queries": pd.Series(dtype="int"),
            "num_users": pd.Series(dtype="int"),
            "table_description": pd.Series(dtype="str"),
            "glossary_terms": pd.Series(dtype="object"),
            "schema": pd.Series(dtype="object"),
        }
    )

    date = (datetime.now() - timedelta(hours=5)).strftime("%Y-%m-%d")

    records: List[DataRecord] = scrape_urn_metadata(context, graph, limit=-1)

    tag_freq_map = {}

    term_freq_map = {}

    for record in records:

        for tag in record.tags:
            tag_freq_map[tag] = tag_freq_map.get(tag, 0) + 1

        for term in record.glossary_terms:
            term_freq_map[term] = term_freq_map.get(term, 0) + 1

    context.log.info(f"tag_freq_map: {tag_freq_map}")

    context.log.info(f"term_freq_map: {term_freq_map}")

    all_tags_in_graph = [
        x["entity"]["urn"] for x in get_all_urns(context, entity_type="TAG")
    ]

    all_terms_in_graph = [
        x["entity"]["urn"] for x in get_all_urns(context, entity_type="GLOSSARY_TERM")
    ]

    for tag in all_tags_in_graph:
        send_datadog_gauge_metric(
            metric=f"{METRIC_PREFIX}.dataset.tag.count",
            value=tag_freq_map.get(tag, 0),
            tags=[f"tag:{tag}"],
        )

    for term in all_terms_in_graph:
        send_datadog_gauge_metric(
            metric=f"{METRIC_PREFIX}.dataset.glossary_term.count",
            value=term_freq_map.get(term, 0),
            tags=[f"glossary_term:{term}"],
        )

    # emit total number of datasets in graph
    send_datadog_gauge_metric(
        metric=f"{METRIC_PREFIX}.dataset.count",
        value=len(records),
        tags=[],
    )

    queries_query = """
                query listQueries($input: ListQueriesInput!) {
                        listQueries(input: $input) {
                            queries {
                            properties {
                                name
                                statement {
                                value
                                }
                            }
                            subjects {
                                dataset {
                                urn
                                }
                            }
                            }
                        }
                    }
"""

    queries, err = run_graphql(
        graph, queries_query, {"input": {"query": "*", "start": 0, "count": 1000}}
    )

    if err:
        context.log.error(err)

    queries = queries.get("listQueries", {}).get("queries", [])

    highlighted_query_count_map = dict(
        Counter([x["subjects"][0]["dataset"]["urn"] for x in queries])
    )

    new_rows = []

    i = 0

    for record in records:

        (
            dataset_urn,
            has_schema,
            storage_location,
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
        ) = astuple(record)

        i += 1
        if i % 100 == 0:
            context.log.info(f"processed {i} of {len(records)}")
        catalog, database, table = get_db_and_table(dataset_urn)

        if valid_glue_schema:
            valid_glue_schema = True if valid_glue_schema.lower() == "true" else False
        else:
            valid_glue_schema = False

        certified = False
        if glossary_terms and (
            "1dd7fa08-22a3-4e10-95d3-b671b0100439" in str(glossary_terms).lower()
        ):
            certified = True

        beta = False
        if glossary_terms and ("glossary_term_beta" in str(glossary_terms).lower()):
            beta = True

        new_row = {
            "urn": dataset_urn,
            "database": database,
            "table": table,
            "has_schema": has_schema,
            "storage_location": storage_location,
            "domain": domain,
            "last_update": last_update,
            "table_type": table_type,
            "valid_glue_schema": valid_glue_schema,
            "num_assertions": num_assertions,
            "num_queries": num_queries,
            "num_users": num_users,
            "table_description": table_description,
            "glossary_terms": glossary_terms,
            "datahub_schema": merged_schema,
            "sql_sources_schema": sql_sources_schema,
            "github_url": github_url,
            "fields_count": fields_count,
            "fields_with_column_descriptions_count": fields_with_column_descriptions_count,
            "fields_with_quality_column_descriptions_count": fields_with_quality_column_descriptions_count,
            "data_contract_url": data_contract_url,
            "owner": owner,
            "highlighted_query_count": highlighted_query_count_map.get(dataset_urn, 0),
            "certified": certified,
            "beta": beta,
            "incident_status": incident_status,
            "assertion_status": assertion_status,
            "tags": tags,
        }
        new_rows.append(new_row)

    df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)

    df["poll_date_hour"] = pd.to_datetime(datetime.now()).round(freq="H")

    context.log.info(len(df))

    raw_json = df.to_json(orient="records")

    s3_client = boto3.client("s3")

    s3_client.put_object(
        Body=raw_json,
        Bucket="samsara-amundsen-metadata",
        Key=f"staging/scrapes/{get_datahub_env()}/datahub_{date}.json",
    )

    # backup custom graphql queries to s3
    for query_name, query_info in backup_queries.items():
        query_json, err = run_graphql(
            graph, query_info["query"], query_info["variables"]
        )
        if err:
            context.log.error(err)

        current_time = datetime.now().strftime("%Y%m%d%H%M%S")

        s3_client.put_object(
            Body=json.dumps(query_json),
            Bucket="samsara-amundsen-metadata",
            Key=f"staging/scrapes/backup_queries/{get_datahub_env()}/{date}/{query_name}_{current_time}.json",
        )

    log_op_duration(op_start_time)
    return 0
