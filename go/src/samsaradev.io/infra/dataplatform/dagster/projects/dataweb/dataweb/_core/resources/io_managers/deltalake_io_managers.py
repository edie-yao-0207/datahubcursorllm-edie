import os
from collections import OrderedDict
from functools import reduce
from operator import iand
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    InputContext,
    MetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    OutputContext,
    WeeklyPartitionsDefinition,
)
from dagster._core.definitions.backfill_policy import BackfillPolicyType
from dagster._core.definitions.metadata import RawMetadataValue
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from ....userpkgs.constants import WarehouseWriteMode
from ...utils import get_run_env, range_partitions_from_execution_context

DELTALAKE_DATE_FORMAT = "%Y-%m-%d"
DELTALAKE_DAILY_PARTITION_NAME = "date"
DELTALAKE_MONTHLY_PARTITION_NAME = "date_month"

DEFAULT_LOOKBACK_PERIOD = 0

# SCHEMA_VALIDATION_SKIP_LIST is a list of delta tables that are
# excluded from having their schemas validated. This is intended for
# tables that have dependencies on upstream entities with frequently
# changing schemas such as kinesisstats or rds tables.
SCHEMA_VALIDATION_SKIP_LIST = [
    "datamodel_core_bronze.raw_productdb_devices",
    "datamodel_core_bronze.raw_clouddb_organizations",
    "datamodel_core_bronze.raw_clouddb_groups",
    "datamodel_core_bronze.raw_clouddb_sfdc_accounts",
    "datamodel_core_bronze.raw_clouddb_org_sfdc_accounts",
    "datamodel_core_bronze.raw_fueldb_shards_fuel_types",
    "datamodel_core_bronze.raw_vindb_shards_device_vin_metadata",
    "datamodel_core_bronze.raw_productsdb_gateways",
    "datamodel_core_bronze.dagster_costs_evaluation_daily_organizations_snapshot",
    "datamodel_core_bronze.dagster_costs_evaluation_device_heartbeats",
    "datamodel_core_bronze.dagster_costs_evaluation_fleet_device_attributes",
    "datamodel_plaform_bronze.stg_datastreams_api_events",
    "datamodel_platform_bronze.stg_datastreams_mobile_events",
    "datamodel_platform_bronze.raw_clouddb_users",
    "datamodel_platform_bronze.raw_clouddb_drivers",
    "datamodel_platform_bronze.raw_clouddb_users_organizations",
    "datamodel_platform_bronze.raw_clouddb_custom_roles",
    "datamodel_platform_bronze.raw_workflowsdb_shards_workflow_configs",
    "datamodel_platform_bronze.raw_workflowsdb_shards_actions",
    "datamodel_platform_bronze.raw_workflowsdb_shards_triggers",
    "datamodel_platform_bronze.raw_workflowsdb_shards_trigger_targets",
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


class DeltaTableIOManager:
    """InlineDeltaTableIOManager is for reading and writing spark DataFrame objects as delta formatted tables withinin an asset function.

    InlineDeltaTableIOManager references properties passed thorugh context.metadata for controlling read and write operations. If an asset is partitioned,
    InlineDeltaTableIOManager will only load and write the partitions passed in through Dagster by default. Set the 'lookback_period' property of context.metadata
    to load or write partitions beyond the partition window passed in by Dagster.

    By default, InlineDeltaTableIOManager will use the 'overwrite' mode when
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

    def _get_path(self, context: Union[OutputContext, AssetExecutionContext]) -> str:
        database = context.asset_key.path[-2]
        table = context.asset_key.path[-1]
        return f"{database}.{table}"

    def _get_output_path(
        self, context: Union[OutputContext, AssetExecutionContext]
    ) -> str:
        if get_run_env() != "prod":
            database = os.getenv("WRITE_DB", "datamodel_dev")
        else:
            database = context.asset_key.path[-2]
        table = context.asset_key.path[-1]
        return f"{database}.{table}"

    def _get_lookback_period(self, context):
        return context.metadata.get("lookback_period", DEFAULT_LOOKBACK_PERIOD)

    def _schema(self, metadata) -> StructType:
        return StructType.fromJson(
            {
                "fields": [
                    self._transform_col(column)
                    for column in metadata.get("schema") or {}
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
        elif isinstance(column_type, dict) and column_type["type"] == "map":
            spark_type = {
                "type": "map",
                "keyType": column_type["keyType"],  # Keys are always simple types
                "valueType": self._element_type(
                    column_type["valueType"], self._transform_col
                ),
                "valueContainsNull": column_type.get("valueContainsNull", False),
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
                "fields": sorted(
                    [transform_func(c) for c in column["fields"]],
                    key=lambda item: item["name"],
                ),
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
                "elementType": self._cleanse_element_type(field_type["elementType"]),
            }
        elif isinstance(field_type, dict) and field_type["type"] == "map":
            spark_type = {
                "type": "map",
                "keyType": field_type["keyType"],  # Keys are always simple types
                "valueType": self._cleanse_element_type(field_type["valueType"]),
                "valueContainsNull": field_type.get("valueContainsNull", False),
            }
        else:
            spark_type = field_type

        return {
            "name": field["name"],
            "type": spark_type,
            # "metadata": field.get("metadata", {}),
        }

    def _cleanse_element_type(
        self, element: Dict[str, Union[str, bool, Dict]]
    ) -> Dict[str, Union[str, bool, Dict]]:
        """Recursively cleanse metadata from nested element types (structs, arrays, maps)."""
        if isinstance(element, dict):
            if element.get("type") == "struct":
                return {
                    "type": "struct",
                    "fields": sorted(
                        [self._cleanse_schema(c) for c in element["fields"]],
                        key=lambda item: item["name"],
                    ),
                }
            elif element.get("type") == "array":
                return {
                    "type": "array",
                    "elementType": self._cleanse_element_type(element["elementType"]),
                }
            elif element.get("type") == "map":
                return {
                    "type": "map",
                    "keyType": element["keyType"],
                    "valueType": self._cleanse_element_type(element["valueType"]),
                    "valueContainsNull": element.get("valueContainsNull", False),
                }
        return element

    def _create_catalog_table(
        self,
        context: Union[OutputContext, AssetExecutionContext],
        metadata: Dict[str, Any],
        format: str = "delta",
    ) -> None:
        schema = self._schema(metadata)
        table_description = metadata.get("description", "")
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
        context: Union[OutputContext, AssetExecutionContext],
        schema_actual: Sequence[Mapping[str, Any]],
        schema_expected: Sequence[Mapping[str, Any]],
    ) -> None:
        """Compare actual DataFrame schema with expected schema and log detailed differences.

        This method performs a comprehensive comparison between the actual schema
        (from a Spark DataFrame) and the expected schema (from asset metadata).
        It identifies three types of schema mismatches:

        1. Missing fields: Expected fields that are not present in actual data
        2. Extra fields: Actual fields that are not expected in the schema definition
        3. Different fields: Fields that exist in both but have different type definitions

        All differences are logged as errors with detailed field-by-field information
        to help diagnose schema validation failures.

        Args:
            context: Dagster execution context for logging
            schema_actual: Actual schema from the Spark DataFrame as a sequence of field mappings
            schema_expected: Expected schema from asset metadata as a sequence of field mappings

        Returns:
            None: This method only logs differences and doesn't return values
        """
        schema_actual_map = {elem["name"]: elem for elem in schema_actual}
        schema_expected_map = {elem["name"]: elem for elem in schema_expected}
        missing_fields = []
        extra_fields = []
        different_fields = []

        # Check for missing and different fields (expected → actual)
        for key, expected_field in schema_expected_map.items():
            actual_field = schema_actual_map.get(key)
            if actual_field is None:
                missing_fields.append(key)
            elif expected_field != actual_field:
                different_fields.append((key, actual_field, expected_field))

        # Check for extra fields (actual → expected)
        for key in schema_actual_map.keys():
            if key not in schema_expected_map:
                extra_fields.append(key)

        # Log detailed comparison results
        if len(missing_fields) > 0:
            context.log.error(
                f"Output dataset schema for {context.asset_key} is MISSING expected fields: {missing_fields}"
            )

        if len(extra_fields) > 0:
            context.log.error(
                f"Output dataset schema for {context.asset_key} has EXTRA unexpected fields: {extra_fields}"
            )

        if len(different_fields) > 0:
            context.log.error(
                f"Output dataset schema for {context.asset_key} has DIFFERENT field definitions:"
            )
            for field_name, actual_field, expected_field in different_fields:
                context.log.error(
                    f"  Field '{field_name}':\n    Actual:   {actual_field}\n    Expected: {expected_field}"
                )
        return

    def _validate_table_schema(
        self, context: AssetExecutionContext, dataframe: DataFrame
    ) -> None:

        if isinstance(context, OutputContext):
            metadata = context.definition_metadata
        elif isinstance(context, AssetExecutionContext):
            metadata = context.assets_def.metadata_by_key.get(context.asset_key, {})

        expected_schema = [
            self._transform_col(column) for column in metadata.get("schema") or {}
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
            context.log.error(
                f"""CLEANED SCHEMA COMPARISON:
                Expected schema (cleaned & sorted): {expected_schema}

                Actual schema (cleaned & sorted):   {actual_schema}

                Raw actual schema (uncleaned): {dataframe.schema.jsonValue()['fields']}
                """
            )

            raise ValueError("Actual table schema does not equal expected table schema")

    def _update_table_description(
        self,
        context: Union[OutputContext, AssetExecutionContext],
        current_table_description: str,
        new_table_description: str,
    ):
        spark = self.spark_session
        new_comment = new_table_description
        table_name = self._get_output_path(context)

        if new_comment == current_table_description:
            context.log.info(
                f"Current description ({current_table_description}) already set for {table_name}, skipping"
            )
        else:
            spark.sql(f"COMMENT ON TABLE {table_name} IS '{new_comment}'")
            context.log.info(
                f"Set new description: ({new_comment})\n for table: {table_name}"
            )

    def handle_output(
        self, context: Union[OutputContext, AssetExecutionContext], dataframe: DataFrame
    ) -> Mapping[str, RawMetadataValue]:
        if os.getenv("MODE") == "DRY_RUN":
            context.log.info("dry run - exiting without materialization")
            return {"dry_run_early_exit": MetadataValue.int(0)}

        table_name = self._get_output_path(context)
        context.log.info(f"writing to target table: {table_name}")

        if table_name not in SCHEMA_VALIDATION_SKIP_LIST:
            self._validate_table_schema(context, dataframe)
        else:
            context.log.info("Skipping schema validation...")

        partition_dimensions = []
        partition_keys_by_dimension = OrderedDict()

        partitions_def = None
        partition_keys = []
        metadata = {}
        if isinstance(context, OutputContext):
            partitions_def = (
                context.asset_partitions_def if context.has_asset_partitions else None
            )
            metadata = context.definition_metadata
        elif isinstance(context, AssetExecutionContext):
            partitions_def = context.assets_def.partitions_def
            metadata = context.assets_def.metadata_by_key.get(context.asset_key, {})

        if partitions_def:

            if isinstance(context, OutputContext):
                partition_keys = context.asset_partition_keys
            elif isinstance(context, AssetExecutionContext):
                if (
                    isinstance(partitions_def, MultiPartitionsDefinition)
                    and context.assets_def.backfill_policy is not None
                    and context.assets_def.backfill_policy.policy_type
                    == BackfillPolicyType.SINGLE_RUN
                ):
                    # For single-run backfills, Dagster provides a range of partitions to materialize. In this case we need to get all
                    # the partition names between partition_range_start and partition_range_end.
                    context.log.info("Getting partitions for single run backfill")

                    partition_keys = range_partitions_from_execution_context(context)

                    context.log.info(
                        f"Writing {len(partition_keys)} partitions from '{partition_keys[0]}' to '{partition_keys[-1]}'"
                    )
                else:
                    partition_keys = context.partition_keys

            if isinstance(partitions_def, DailyPartitionsDefinition):
                assert (
                    DELTALAKE_DAILY_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily, weekly, or monthly partition definition. The column for daily partitions must be named '{DELTALAKE_DAILY_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_DAILY_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_DAILY_PARTITION_NAME
                ] = partition_keys

            elif isinstance(partitions_def, WeeklyPartitionsDefinition):
                assert (
                    DELTALAKE_DAILY_PARTITION_NAME in dataframe.columns
                ), f"All single-dimension partitioned assets must have a daily, weekly, or monthly partition definition. The column for weekly partitions must be named '{DELTALAKE_DAILY_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_DAILY_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_DAILY_PARTITION_NAME
                ] = partition_keys

            elif isinstance(partitions_def, MonthlyPartitionsDefinition):
                assert (
                    DELTALAKE_MONTHLY_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily, weekly, or monthly partition definition. The column must be named '{DELTALAKE_MONTHLY_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_MONTHLY_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_MONTHLY_PARTITION_NAME
                ] = partition_keys

            elif isinstance(partitions_def, MultiPartitionsDefinition):
                assert (
                    partitions_def.partitions_defs[0].name
                    in (
                        DELTALAKE_DAILY_PARTITION_NAME,
                        DELTALAKE_MONTHLY_PARTITION_NAME,
                    )
                    and isinstance(
                        partitions_def.partitions_defs[0].partitions_def,
                        (
                            DailyPartitionsDefinition,
                            WeeklyPartitionsDefinition,
                            MonthlyPartitionsDefinition,
                        ),
                    )
                    and (
                        DELTALAKE_DAILY_PARTITION_NAME in dataframe.columns
                        or DELTALAKE_MONTHLY_PARTITION_NAME in dataframe.columns
                    )
                ), f"All multi dimension partitioned assets must have a daily, weekly, or monthly primary partition definition. The column must be named '{DELTALAKE_DAILY_PARTITION_NAME}' or '{DELTALAKE_MONTHLY_PARTITION_NAME}'."

                partition_dimensions = partitions_def.partition_dimension_names

                primary_partition_keys = set()
                secondary_partition_keys = set()

                for item in partition_keys:
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

        primary_keys = metadata.get("primary_keys") or []
        mode = metadata.get("write_mode", WarehouseWriteMode.OVERWRITE)

        context.log.info("Check if table exist")
        options = {"mergeSchema": "true"}
        table_exists = self._table_exists(context)
        if not table_exists:
            context.log.info("Table does not exist.. Creating table in catalog.")
            self._create_catalog_table(context, metadata)
            mode = WarehouseWriteMode.OVERWRITE
            options = {"overwriteSchema": "true"}

        spark = self.spark_session

        if mode == WarehouseWriteMode.OVERWRITE:
            df_writer = df.write.format("delta").mode(mode)

            if partitions_def:
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

        elif mode == WarehouseWriteMode.MERGE:
            spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
            source = df.alias("source")
            target = DeltaTable.forName(spark, table_name).alias("target")

            match_conditions = [
                F.col(f"source.{pk}") == F.col(f"target.{pk}") for pk in primary_keys
            ]
            if partitions_def:
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
            self._update_table_description(
                context, current_table_comment, metadata.get("description", "")
            )
        except Exception as e:
            context.log.error(e)
            context.log.error("could not update table descripion")

        return

    def load_input(self, context: InputContext) -> DataFrame:
        spark = self.spark_session
        dataframe = spark.table(self._get_path(context.upstream_output))

        if context.has_asset_partitions:

            partitions_def = context.asset_partitions_def
            partition_dimensions = []
            partition_keys_by_dimension = OrderedDict()

            if isinstance(partitions_def, DailyPartitionsDefinition):
                assert (
                    DELTALAKE_DAILY_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily or monthly partition definition. The column must be named '{DELTALAKE_DAILY_PARTITION_NAME}' or '{DELTALAKE_MONTHLY_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_DAILY_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_DAILY_PARTITION_NAME
                ] = context.asset_partition_keys

            elif isinstance(partitions_def, MonthlyPartitionsDefinition):
                assert (
                    DELTALAKE_MONTHLY_PARTITION_NAME in dataframe.columns
                ), f"All single dimension partitioned assets must have a daily or monthly partition definition. The column must be named '{DELTALAKE_DAILY_PARTITION_NAME}' or '{DELTALAKE_MONTHLY_PARTITION_NAME}'."

                partition_dimensions = [
                    DELTALAKE_MONTHLY_PARTITION_NAME,
                ]
                partition_keys_by_dimension[
                    DELTALAKE_MONTHLY_PARTITION_NAME
                ] = context.asset_partition_keys

            elif isinstance(partitions_def, MultiPartitionsDefinition):
                assert (
                    partitions_def.partitions_defs[0].name
                    in (
                        DELTALAKE_DAILY_PARTITION_NAME,
                        DELTALAKE_MONTHLY_PARTITION_NAME,
                    )
                    and isinstance(
                        partitions_def.partitions_defs[0].partitions_def,
                        (DailyPartitionsDefinition, MonthlyPartitionsDefinition),
                    )
                    and (
                        DELTALAKE_DAILY_PARTITION_NAME in dataframe.columns
                        or DELTALAKE_MONTHLY_PARTITION_NAME in dataframe.columns
                    )
                ), f"All multi dimension partitioned assets must have a daily or monthly primary partition definition. The column must be named '{DELTALAKE_DAILY_PARTITION_NAME}' or '{DELTALAKE_MONTHLY_PARTITION_NAME}'."
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
