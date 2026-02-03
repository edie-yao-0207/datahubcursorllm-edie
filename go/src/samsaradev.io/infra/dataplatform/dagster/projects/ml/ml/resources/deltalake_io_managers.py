import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import reduce
from operator import iand
from typing import Any, Callable, Dict, Mapping, Sequence, Union

import boto3
from dagster import (
    ConfigurableIOManager,
    DailyPartitionsDefinition,
    InputContext,
    MetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    OutputContext,
    TableColumn,
    TableSchema,
)
from dagster._core.definitions.metadata import RawMetadataValue
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from ..common.utils import WarehouseWriteMode, get_run_env, log_query_data

DELTALAKE_DATE_FORMAT = "%Y-%m-%d"
DELTALAKE_DATE_PARTITION_NAME = "date"

WRITE_MODE_OVERWRITE = "overwrite"
WRITE_MODE_MERGE = "merge"

DEFAULT_LOOKBACK_PERIOD = 0

# SCHEMA_VALIDATION_SKIP_LIST is a list of delta tables that are
# excluded from having their schemas validated. This is intended for
# tables that have dependencies on upstream entities with frequently
# changing schemas such as kinesisstats or rds tables.
SCHEMA_VALIDATION_SKIP_LIST = [
    "datamodel_core_bronze.raw_productdb_devices",
    "datamodel_dev.raw_productdb_devices",
    "dataplatform_dev.stg_asset_before_dq",
    "datamodel_core_bronze.raw_clouddb_organizations",
    "datamodel_core_bronze.raw_clouddb_groups",
    "datamodel_core_bronze.raw_clouddb_sfdc_accounts",
    "datamodel_core_bronze.raw_clouddb_org_sfdc_accounts",
    "datamodel_core_bronze.raw_fueldb_shards_fuel_types",
    "datamodel_core_bronze.raw_vindb_shards_device_vin_metadata",
    "datamodel_safety.fct_safety_events_lifecycle",
    "datamodel_dev.raw_clouddb_organizations",
    "datamodel_dev.raw_clouddb_groups",
    "datamodel_dev.raw_clouddb_sfdc_accounts",
    "datamodel_dev.raw_clouddb_org_sfdc_accounts",
    "datamodel_dev.raw_fueldb_shards_fuel_types",
    "datamodel_dev.raw_vindb_shards_device_vin_metadata",
    "datamodel_dev.lifetime_device_activity",
    "datamodel_core.lifetime_device_activity",
    "datamodel_dev.fct_safety_events_lifecycle",
    "datamodel_telematics.fct_trips",
    "datamodel_telematics.fct_trips_daily",
    "datamodel_platform_bronze.raw_clouddb_users",
    "datamodel_platform_bronze.raw_clouddb_drivers",
    "datamodel_platform_bronze.raw_clouddb_users_organizations",
    "datamodel_platform_bronze.raw_clouddb_custom_roles",
    "datamodel_dev.raw_clouddb_users",
    "datamodel_dev.raw_clouddb_drivers",
    "datamodel_dev.raw_clouddb_users_organizations",
    "datamodel_dev.raw_clouddb_custom_roles",
    "datamodel_platform_bronze.raw_workflowsdb_shards_workflow_configs",
    "datamodel_platform_bronze.raw_workflowsdb_shards_actions",
    "datamodel_platform_bronze.raw_workflowsdb_shards_triggers",
    "datamodel_platform_bronze.raw_workflowsdb_shards_trigger_targets",
    "datamodel_platform_silver.stg_alert_configs",
    "datamodel_platform.dim_alert_configs",
    "datamodel_dev.raw_workflowsdb_shards_workflow_configs",
    "datamodel_dev.raw_workflowsdb_shards_actions",
    "datamodel_dev.raw_workflowsdb_shards_triggers",
    "datamodel_dev.raw_workflowsdb_shards_trigger_targets",
    "datamodel_dev.stg_alert_configs",
    "datamodel_dev.dim_alert_configs",
    "datamodel_dev.raw_productsdb_gateways",
    "datamodel_core_bronze.raw_productsdb_gateways",
    "datamodel_dev.dagster_costs_evaluation_daily_organizations_snapshot",
    "datamodel_dev.dagster_costs_evaluation_device_heartbeats",
    "datamodel_dev.dagster_costs_evaluation_fleet_device_attributes",
    "datamodel_core_bronze.dagster_costs_evaluation_daily_organizations_snapshot",
    "datamodel_core_bronze.dagster_costs_evaluation_device_heartbeats",
    "datamodel_core_bronze.dagster_costs_evaluation_fleet_device_attributes",
    "datamodel_dev.stg_devices_settings_first_on_date",
    "datamodel_dev.stg_devices_settings_latest_date",
    "datamodel_core_silver.stg_devices_settings_first_on_date",
    "datamodel_core_silver.stg_devices_settings_latest_date",
    "datamodel_dev.stg_weather_alerts",
    "datamodel_telematics_silver.stg_weather_alerts",
    "datamodel_dev.raw_productsdb_gateway_device_history",
    "datamodel_core_bronze.raw_productsdb_gateway_device_history",
]


def get_column_descriptions(spark, table):
    result = {}

    query = f"DESCRIBE TABLE {table}"
    df = spark.sql(query)

    rows = df.collect()
    for row in rows:
        result[row["col_name"]] = row["comment"]

    return result


def get_table_comment(spark, table_name):
    # Describe the table and convert to Pandas DataFrame for easier querying
    description_df = spark.sql(f"DESCRIBE FORMATTED {table_name}").toPandas()

    # Filter the DataFrame to find the row with the table comment
    comment_row = description_df[description_df["col_name"] == "Comment"]

    # If the comment row exists, return the comment, else return None
    if not comment_row.empty:
        return comment_row["data_type"].values[0]
    else:
        return None


class DeltaTableIOManager(ConfigurableIOManager):
    """A Dagster IO Manager for reading and writing spark DataFrame objects as delta formatted tables.

    DeltaTableIOManager references properties passed thorugh context.definition_metadata for controlling read and write operations. If an asset is partitioned,
    DeltaTableIOManager will only load and write the partitions passed in through Dagster by default. Set the 'lookback_period' property of context.definition_metadata
    to load or write partitions beyond the partition window passed in by Dagster. By default, DeltaTableIOManager will use the 'overwrite' mode when
    writing DataFrame to table.

    """

    @property
    def spark_session(self) -> SparkSession:
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        return spark

    def _table_exists(self, context) -> bool:
        spark = self.spark_session
        database, table = self._get_output_path(context).split(".")
        # check to see if table exists, supports managed/external
        table_exists = (
            spark.sql(f"SHOW TABLES IN {database}")
            .filter(f"tableName = '{table}'")
            .count()
            > 0
        )
        return table_exists

    def _get_path(self, context) -> str:
        database = (
            context.definition_metadata.get("database") or context.asset_key.path[-2]
        )
        table = context.asset_key.path[-1]
        return f"{database}.{table}"

    def _get_output_path(self, context) -> str:
        if get_run_env() != "prod":
            database = os.getenv("WRITE_DB", "datamodel_dev")
        else:
            database = (
                context.definition_metadata.get("database")
                or context.asset_key.path[-2]
            )
        table = context.asset_key.path[-1]
        return f"{database}.{table}"

    def _get_lookback_period(self, context):
        return context.definition_metadata.get(
            "lookback_period", DEFAULT_LOOKBACK_PERIOD
        )

    def _schema(self, context) -> StructType:
        return StructType.fromJson(
            {
                "fields": [
                    self._transform_col(column)
                    for column in context.definition_metadata.get("schema") or {}
                ]
            }
        )

    def _transform_col(self, column: Dict[str, Any]) -> Dict[str, Any]:
        column_type = column["type"]
        if isinstance(column_type, dict) and column_type["type"] == "struct":
            spark_type = {
                "type": column_type["type"],
                "fields": [self._transform_col(c) for c in column_type["fields"]],
            }
        elif isinstance(column_type, dict) and column_type["type"] == "array":
            spark_type = {
                "type": "array",
                "elementType": self._element_type(
                    column_type["elementType"], self._transform_col
                ),
                "containsNull": True,
            }
        else:
            spark_type = column_type

        return {
            "name": column["name"],
            "type": spark_type,
            "nullable": column.get("nullable", True),
            "metadata": column.get("metadata", {}),
        }

    def _element_type(
        self, column: Dict[str, Union[str, bool, Dict]], transform_func: Callable
    ) -> Dict[str, Union[str, bool, Dict]]:
        if "fields" in column:
            return {
                "type": "struct",
                "fields": [transform_func(c) for c in column["fields"]],
            }
        return column

    def _cleanse_schema(
        self, field: Mapping[str, Union[str, Dict]]
    ) -> Mapping[str, Union[str, Dict]]:
        field_type = field["type"]
        if isinstance(field_type, dict) and field_type["type"] == "struct":
            spark_type = {
                "type": field_type["type"],
                "fields": sorted(
                    [self._cleanse_schema(c) for c in field_type["fields"]],
                    key=lambda item: item["name"],
                ),
            }
        elif isinstance(field_type, dict) and field_type["type"] == "array":
            spark_type = {
                "type": "array",
                "elementType": self._element_type(
                    field_type["elementType"], self._cleanse_schema
                ),
            }
        else:
            spark_type = field_type

        return {
            "name": field["name"],
            "type": spark_type,
            # "metadata": field.get("metadata", {}),
        }

    def _create_catalog_table(
        self,
        context: OutputContext,
        format: str = "delta",
    ) -> None:
        schema = self._schema(context)
        table_description = context.definition_metadata.get("description", "")
        spark = self.spark_session
        spark.catalog.createTable(
            tableName=self._get_output_path(context),
            schema=schema,
            format=format,
            description=table_description,
        )
        return

    def _compare_schema(
        self,
        context,
        schema_actual: Sequence[Mapping[str, Any]],
        schema_expected: Sequence[Mapping[str, Any]],
    ) -> None:
        schema_actual_map = {elem["name"]: elem for elem in schema_actual}
        schema_expected_map = {elem["name"]: elem for elem in schema_expected}
        missing_fields = []
        different_fields = []
        for key, expected_field in schema_expected_map.items():
            actual_field = schema_actual_map.get(key)
            if actual_field is None:
                missing_fields.append(key)
            elif expected_field != actual_field:
                different_fields.append((actual_field, expected_field))
        if len(missing_fields) > 0:
            context.log.error(
                f"Output dataset schema for {context.asset_key} missing expected fields ({missing_fields})"
            )
        if len(different_fields) > 0:
            context.log.error(
                f"Output dataset schema for {context.asset_key} contains fields:\n {list(zip(*different_fields))[0]} \n\n that do not match expected dataset schema:\n\n {list(zip(*different_fields))[1]}"
            )
        return

    def _validate_table_schema(self, context, dataframe: DataFrame) -> None:
        expected_schema = [
            self._transform_col(column)
            for column in context.definition_metadata.get("schema") or {}
        ]
        actual_schema = dataframe.schema.jsonValue()["fields"]

        # We remove the metadata and nullable fields in the schemas before comparing because:
        # 1) the schema from the JSON definition can include a metadata dict field (for metadata management in Glue)
        # while the schema from running the SQL won't contain any comments so it would fail;
        # 2) the inferred schema from the SQL transformation doesn't reflect the true nullability constraint of the table.
        expected_schema = sorted(
            [self._cleanse_schema(c) for c in expected_schema],
            key=lambda item: item["name"],
        )
        actual_schema = sorted(
            [self._cleanse_schema(c) for c in actual_schema],
            key=lambda item: item["name"],
        )

        context.log.info(f"Schema valid: {actual_schema == expected_schema}")

        if actual_schema != expected_schema:
            context.log.error(
                "Actual table schema does not equal expected table schema"
            )
            self._compare_schema(context, actual_schema, expected_schema)
            context.log.info(
                f"expected schema to use is:\n {dataframe.schema.jsonValue()['fields']}"
            )

            raise ValueError("Actual table schema does not equal expected table schema")

    def _update_table_description(
        self, context: OutputContext, current_table_description: str
    ):
        spark = self.spark_session
        new_comment = context.definition_metadata.get("description", "")
        table_name = self._get_output_path(context)

        new_escaped_col_comment = new_comment.replace("'", "`").replace('"', '\\"')

        if new_escaped_col_comment == current_table_description:
            context.log.info(
                f"Current description ({current_table_description}) already set for {table_name}, skipping"
            )
        else:
            spark.sql(f"COMMENT ON TABLE {table_name} IS '{new_escaped_col_comment}'")
            context.log.info(
                f"Set new description: ({new_escaped_col_comment})\n for table: {table_name}"
            )

    def handle_output(
        self, context: OutputContext, dataframe: DataFrame
    ) -> Mapping[str, RawMetadataValue]:
        if os.getenv("MODE") == "DRY_RUN":
            context.log.info("dry run - exiting without materialization")
            return {"dry_run_early_exit": MetadataValue.int(0)}

        table_name = self._get_output_path(context)
        context.log.info(f"writing to target table: {table_name}")

        sql_query = context.definition_metadata.get("sql_query")
        if sql_query != "dynamic":
            log_query_data(context, sql_query)

        if table_name not in SCHEMA_VALIDATION_SKIP_LIST:
            self._validate_table_schema(context, dataframe)
        else:
            context.log.info("Skipping schema validation...")

        partition_dimensions = []
        partition_keys_by_dimension = OrderedDict()
        is_partitioned = context.has_asset_partitions

        if is_partitioned:

            partitions_def = context.asset_partitions_def

            if isinstance(
                partitions_def, (DailyPartitionsDefinition, MonthlyPartitionsDefinition)
            ):
                assert (
                    DELTALAKE_DATE_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily or monthly partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_DATE_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_DATE_PARTITION_NAME
                ] = context.asset_partition_keys

            elif isinstance(partitions_def, MultiPartitionsDefinition):
                assert (
                    partitions_def.partitions_defs[0].name
                    == DELTALAKE_DATE_PARTITION_NAME
                    and isinstance(
                        partitions_def.partitions_defs[0].partitions_def,
                        (DailyPartitionsDefinition, MonthlyPartitionsDefinition),
                    )
                    and DELTALAKE_DATE_PARTITION_NAME in dataframe.columns
                ), f"All multi dimension partitioned assets must have a daily or monthly primary partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."

                partition_dimensions = partitions_def.partition_dimension_names

                primary_partition_keys = set()
                secondary_partition_keys = set()

                for item in context.asset_partition_keys:
                    primary_partition_keys.add(item.split("|")[0])
                    secondary_partition_keys.add(item.split("|")[1])
                partition_keys_by_dimension[partition_dimensions[0]] = list(
                    primary_partition_keys
                )
                partition_keys_by_dimension[partition_dimensions[1]] = list(
                    secondary_partition_keys
                )

            else:
                raise ValueError(
                    f"We currently do not support {partitions_def} partition schemes."
                )

            for (
                partition_dimension,
                partition_keys,
            ) in partition_keys_by_dimension.items():
                dataframe = dataframe.where(
                    F.col(partition_dimension).isin(partition_keys)
                )

        df = dataframe.cache()
        count = df.count()

        primary_keys = context.definition_metadata.get("primary_keys") or []
        mode = context.definition_metadata.get("write_mode", WRITE_MODE_OVERWRITE)
        if isinstance(mode, WarehouseWriteMode):
            mode = mode.value

        context.log.info("Check if table exist")
        options = {"mergeSchema": "true"}
        table_exists = self._table_exists(context)
        if not table_exists:
            context.log.info("Table does not exist.. Creating table in catalog.")
            self._create_catalog_table(context)
            mode = WRITE_MODE_OVERWRITE
            options = {"overwriteSchema": "true"}

        spark = self.spark_session

        if mode == WRITE_MODE_OVERWRITE:
            df_writer = df.write.format("delta").mode(mode)

            if is_partitioned:
                replacement_clause = "1=1"
                for (
                    partition_dimension,
                    partition_keys,
                ) in partition_keys_by_dimension.items():
                    value_str = ",".join(
                        [
                            f"'{item}'" if isinstance(item, str) else f"{item}"
                            for item in partition_keys
                        ]
                    )
                    replacement_clause += f" AND {partition_dimension} IN ({value_str})"

                options = (
                    dict(options, **{"replaceWhere": replacement_clause})
                    if table_exists
                    else options
                )
                df_writer.partitionBy(*partition_dimensions)

            df_writer.options(**options).saveAsTable(table_name)

        elif mode == WRITE_MODE_MERGE:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            source = df.alias("source")
            target = DeltaTable.forName(spark, table_name).alias("target")

            match_conditions = [
                F.col(f"source.{pk}") == F.col(f"target.{pk}") for pk in primary_keys
            ]
            if is_partitioned:
                for (
                    partition_dimension,
                    partition_keys,
                ) in partition_keys_by_dimension.items():
                    match_conditions.append(
                        F.col(f"target.{partition_dimension}").isin(partition_keys)
                    )

            condition = reduce(iand, match_conditions)

            target.merge(
                source=source, condition=condition
            ).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()

        current_column_descriptions = get_column_descriptions(spark, table_name)
        current_table_comment = get_table_comment(spark, table_name)
        context.log.info("current column descriptions: %s", current_column_descriptions)
        context.log.info("current table comment: %s", current_table_comment)

        try:
            self._update_table_description(context, current_table_comment)
        except Exception as e:
            context.log.error(e)
            context.log.error("could not update table descripion")

        return {
            "dataframe_columns": MetadataValue.table_schema(
                TableSchema(
                    columns=[
                        TableColumn(name=field.name, type=field.dataType.typeName())
                        for field in df.schema.fields
                    ]
                )
            ),
            "rows_output": MetadataValue.int(count),
        }

    def load_input(self, context: InputContext) -> DataFrame:
        spark = self.spark_session
        dataframe = spark.table(self._get_path(context.upstream_output))

        if context.has_asset_partitions:

            partitions_def = context.asset_partitions_def
            partition_dimensions = []
            partition_keys_by_dimension = OrderedDict()

            if isinstance(
                partitions_def, (DailyPartitionsDefinition, MonthlyPartitionsDefinition)
            ):
                assert (
                    DELTALAKE_DATE_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily or monthly partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_DATE_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_DATE_PARTITION_NAME
                ] = context.asset_partition_keys

            elif isinstance(partitions_def, MultiPartitionsDefinition):
                assert (
                    partitions_def.partitions_defs[0].name
                    == DELTALAKE_DATE_PARTITION_NAME
                    and isinstance(
                        partitions_def.partitions_defs[0].partitions_def,
                        (DailyPartitionsDefinition, MonthlyPartitionsDefinition),
                    )
                    and DELTALAKE_DATE_PARTITION_NAME in dataframe.columns
                ), f"All multi dimension partitioned assets must have a daily or monthly primary partition definition. The column must be named '{DELTALAKE_DATE_PARTITION_NAME}'."
                partition_dimensions = partitions_def.partition_dimension_names

                primary_partition_keys = set()
                secondary_partition_keys = set()
                for item in context.asset_partition_keys:
                    primary_partition_keys.add(item.split("|")[0])
                    secondary_partition_keys.add(item.split("|")[1])
                partition_keys_by_dimension[partition_dimensions[0]] = list(
                    primary_partition_keys
                )
                partition_keys_by_dimension[partition_dimensions[1]] = list(
                    secondary_partition_keys
                )

            else:
                raise ValueError(
                    f"We currently do not support {partitions_def} partition schemes."
                )

            for (
                partition_dimension,
                partition_keys,
            ) in partition_keys_by_dimension.items():
                dataframe = dataframe.where(
                    F.col(partition_dimension).isin(partition_keys)
                )

        return dataframe
