from typing import Any, Callable, Dict, List, Optional, TypedDict, Union

from dagster import AssetDep, AssetExecutionContext, AssetKey, AssetsDefinition, asset
from dagster._core.definitions.asset_key import CoercibleToAssetKey
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from .._core import _checks as checks
from .._core.constants import GlossaryTerm
from .._core.resources.databricks.constants import DEFAULT_SPARK_VERSION
from .._core.resources.databricks.databricks_step_launchers import (
    ConfigurableDatabricksStepLauncher,
)
from .._core.utils import get_code_location, get_run_env
from ..userpkgs.constants import GENERAL_PURPOSE_INSTANCE_POOL_KEY, AWSRegion, Owner


class FieldMetaDataDefinition(TypedDict):
    comment: str


class ExpressionDefinition(TypedDict):
    expression_type: str
    expression: str
    metadata: FieldMetaDataDefinition


class DimensionDefinition(TypedDict):
    name: str
    metadata: FieldMetaDataDefinition


METRIC_ENTITIES_DATABASE = "metrics_repo"
METRIC_FUNCTIONS_DATABASE = "metrics_api"
METRIC_DEV_DATABASE = "datamodel_dev"


def metric_repo_database() -> str:
    return METRIC_ENTITIES_DATABASE if get_run_env() == "prod" else METRIC_DEV_DATABASE


def metric_api_database() -> str:
    return METRIC_FUNCTIONS_DATABASE if get_run_env() == "prod" else METRIC_DEV_DATABASE


def _create_metric_view_statement(
    name: str, definition: str, description: str, schema: List[Dict[str, Any]]
) -> str:
    database = metric_repo_database()

    column_list = []
    for field in schema:
        field_name = field["name"]
        metadata = field.get("metadata", {})
        comment = metadata.get("comment")
        if comment:
            column_list.append(f"""{field_name} COMMENT '{comment}'""")
        else:
            column_list.append(f"{field_name}")

    statement = f"""CREATE OR REPLACE VIEW {database}.{name}
        ({' , '.join(column_list)})
        COMMENT '{description}'
        AS {definition}
        """
    return statement


def _create_parameters(dimensions: List[Dict[str, Any]]) -> str:
    parameters = []

    for field in dimensions:
        name = field["name"]
        if name == "date":  # 'date' is a special dimension and is handled differently
            param = f"date_range ARRAY<STRING> DEFAULT NULL COMMENT 'The start and end date of a date range to filter and group by.'"
        else:
            param = f"{name} ARRAY<STRING> DEFAULT NULL COMMENT 'A list of {name} values to filter and group by.'"
        parameters.append(param)
    return " , ".join(parameters)


def _create_return_schema(
    spark: SparkSession,
    definition: str,
    expression: ExpressionDefinition,
    dimensions: List[Dict[str, Any]],
) -> str:
    parameters = []
    for field in dimensions:
        field_name = field["name"]
        dtype = field["type"]
        metadata = field.get("metadata", {})
        comment = metadata.get("comment", "")
        param = f"{field_name} {dtype.upper()} COMMENT '{comment}'"
        parameters.append(param)

    # Infer the return type of the metric expression
    inferred_schema = (
        spark.sql(definition)
        .groupby()
        .agg(F.expr(expression["expression"]).alias("value"))
        .schema
    )
    field_name, field_type = inferred_schema["value"].simpleString().split(":")
    parameters.append(
        f"{field_name} {field_type.upper()} COMMENT '{expression['metadata']['comment']}'"
    )

    return " , ".join(parameters)


def _create_projection(
    name: str,
    expression: ExpressionDefinition,
    dimensions: List[Dict[str, Any]],
) -> str:
    projections = []
    for field in dimensions:
        field_name = field["name"]
        if field_name == "date":
            projection = f"""CASE
                WHEN {name}.{field_name}_range IS NULL THEN NULL
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 2
                    THEN anon.{field_name}
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'daily'
                    THEN anon.{field_name}
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'weekly'
                    THEN DATE_FORMAT(DATE_TRUNC('WEEK', anon.{field_name}), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'monthly'
                    THEN DATE_FORMAT(DATE_TRUNC('MONTH', anon.{field_name}), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'quarterly'
                    THEN DATE_FORMAT(ADD_MONTHS(DATE_TRUNC('QUARTER', ADD_MONTHS(anon.{field_name}, -1)),1), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'collapsed'
                    THEN NULL
                ELSE RAISE_ERROR('Invaid argument: {field_name}_range must be ARRAY(<start_date>, <end_date> [, <granularity>]).')
                END AS {field_name}
            """
        else:
            projection = f"""CASE
                WHEN {name}.{field_name} IS NULL THEN NULL
                WHEN ARRAY_SIZE({name}.{field_name}) > 0 AND LOWER(ELEMENT_AT({name}.{field_name}, -1)) = 'collapsed' THEN NULL
                ELSE anon.{field_name}
                END AS {field_name}
            """
        projections.append(projection)

    projections.append(f"{expression['expression']} AS value")

    return " , ".join(projections)


def _create_selections(name: str, dimensions: List[Dict[str, Any]]) -> str:
    selections = []
    for field in dimensions:
        field_name = field["name"]
        if field_name == "date":
            selection = f"""CASE
                WHEN ARRAY_SIZE({name}.{field_name}_range) IN (2, 3) THEN anon.{field_name} BETWEEN {name}.{field_name}_range[0] AND {name}.{field_name}_range[1]
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 0 OR {name}.{field_name}_range IS NULL THEN TRUE
                ELSE RAISE_ERROR('Invaid argument: {field_name}_range must be ARRAY(<start_date>, <end_date> [, <granularity>]).')
            END
            """
        else:
            selection = f"""CASE
                WHEN ARRAY_SIZE(ARRAY_REMOVE({name}.{field_name}, 'collapsed')) > 0
                THEN ARRAY_CONTAINS({name}.{field_name}, {field_name}::STRING)
                ELSE TRUE
            END
            """
        selections.append(selection)
    return " AND ".join(selections)


def _create_groupby_clause(name: str, dimensions: List[Dict[str, Any]]) -> str:

    groupings = []
    for field in dimensions:
        field_name = field["name"]
        if field_name == "date":
            grouping = f"""CASE
                WHEN {name}.{field_name}_range IS NULL THEN NULL
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 2
                    THEN anon.{field_name}
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'daily'
                    THEN anon.{field_name}
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'weekly'
                    THEN DATE_FORMAT(DATE_TRUNC('WEEK', anon.{field_name}), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'monthly'
                    THEN DATE_FORMAT(DATE_TRUNC('MONTH', anon.{field_name}), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'quarterly'
                    THEN DATE_FORMAT(ADD_MONTHS(DATE_TRUNC('QUARTER', ADD_MONTHS(anon.{field_name}, -1)),1), 'yyyy-MM-dd')
                WHEN ARRAY_SIZE({name}.{field_name}_range) = 3 AND LOWER({name}.{field_name}_range[2]) = 'collapsed'
                    THEN NULL
                ELSE RAISE_ERROR('Invaid argument: {field_name}_range must be ARRAY(<start_date>, <end_date> [, <granularity>]).')
                END
            """
        else:
            grouping = f"""CASE
                WHEN {name}.{field_name} IS NULL THEN NULL
                WHEN ARRAY_SIZE({name}.{field_name}) > 0 AND LOWER(ELEMENT_AT({name}.{field_name}, -1)) = 'collapsed' THEN NULL
                ELSE anon.{field_name}
                END
            """
        groupings.append(grouping)
    return " , ".join(groupings)


def _create_metric_function_statement(
    spark: SparkSession,
    name: str,
    definition: str,
    expression: ExpressionDefinition,
    schema: List[Dict[str, Any]],
) -> str:

    dimensions = list(
        filter(lambda x: x.get("metadata", {}).get("is_dimension"), schema)
    )

    statement = f"""CREATE OR REPLACE FUNCTION {metric_api_database()}.{name} (
        {_create_parameters(dimensions)}
    )
    RETURNS TABLE({_create_return_schema(spark, definition, expression, dimensions)})
    RETURN SELECT
        {_create_projection(name, expression, dimensions)}
    FROM {metric_repo_database()}.{name} anon
    WHERE
        {_create_selections(name, dimensions)}
    GROUP BY
        {_create_groupby_clause(name, dimensions)}
    """
    return statement


def _create_metric_entity(
    context: AssetExecutionContext,
    spark: SparkSession,
    name: str,
    definition: str,
    description: str,
    schema: List[Dict[str, Any]],
) -> None:
    statement = _create_metric_view_statement(name, definition, description, schema)
    context.log.info(statement)

    spark.sql(statement)
    return


def _create_metric_function(
    context: AssetExecutionContext,
    spark: SparkSession,
    name: str,
    definition: str,
    expression: ExpressionDefinition,
    schema: List[Dict[str, Any]],
) -> None:
    statement = _create_metric_function_statement(
        spark, name, definition, expression, schema
    )
    context.log.info(statement)
    spark.sql(f"DROP FUNCTION IF EXISTS {metric_api_database()}.{name}")
    spark.sql(statement)
    return


def _coerce_dependencies(
    region: str, upstreams: Union[List[str], List[AssetKey], List[AssetDep]]
) -> List[AssetDep]:

    if upstreams is None:
        return upstreams

    deps = []
    for upstream in upstreams:
        partition_mapping = None
        if isinstance(upstream, AssetDep):
            deps.append(upstream)
        elif isinstance(upstream, AssetKey):
            deps.append(AssetDep(asset=upstream, partition_mapping=partition_mapping))
        elif isinstance(upstream, str):
            region_upstream_parts = upstream.split(":")
            if len(region_upstream_parts) > 1:
                regions = region_upstream_parts[0].split("|")
                for r in regions:
                    _key = AssetKey([r] + region_upstream_parts[1].split("."))
                    deps.append(
                        AssetDep(asset=_key, partition_mapping=partition_mapping)
                    )
            else:
                _key = AssetKey(
                    [
                        region,
                    ]
                    + region_upstream_parts[0].split(".")
                )
                deps.append(AssetDep(asset=_key, partition_mapping=partition_mapping))

    for idx, dep in enumerate(deps):
        if dep.asset_key.path[0] not in (
            AWSRegion.US_WEST_2,
            AWSRegion.EU_WEST_1,
            AWSRegion.CA_CENTRAL_1,
        ):
            deps[idx] = AssetDep(
                asset=dep.asset_key.with_prefix(region),
                partition_mapping=dep.partition_mapping,
            )
    return deps


def _update_schema(schema: List[Dict[str, Any]], dimensions: List[Dict[str, Any]]):
    dim_fields_by_name = {field.get("name"): field for field in dimensions}
    updated_schema = []
    for field in schema:
        field_name = field["name"]
        dim_field = dim_fields_by_name.get(field_name)
        if dim_field:
            dim_metadata = dim_field.get("metadata", {})
            dim_metadata["is_dimension"] = True
            field["metadata"] = dict(field["metadata"], **dim_metadata)
        updated_schema.append(field)

    return updated_schema


def metric(
    description: str,
    expressions: List[ExpressionDefinition],
    dimensions: List[DimensionDefinition],
    regions: List[str],
    owners: List[Owner],
    upstreams: Optional[List[CoercibleToAssetKey]],
    glossary_terms: Optional[List[GlossaryTerm]] = None,
) -> List[AssetsDefinition]:
    def _wrapper(asset_fn: Callable):

        _glossary_terms = (
            [f"{term.value}" for term in glossary_terms] if glossary_terms else []
        )

        # check definitions
        checks._check_regions_input(asset_fn.__name__, regions)
        checks._check_asset_name(asset_fn)
        checks._check_asset_upstreams_input(asset_fn.__name__, upstreams)
        checks._check_metric_return_type(asset_fn)
        checks._check_glossary_terms(asset_fn, _glossary_terms)
        checks._check_metric_dimensions(asset_fn, dimensions)
        checks._check_asset_owners(asset_fn.__name__, owners)
        checks._check_asset_description(asset_fn.__name__, description)

        _owners = [f"team:{owner.team}" for owner in owners]

        metadata = {
            "dimensions": dimensions,
            "owners": _owners,
            "glossary_terms": _glossary_terms,
        }

        assets = []
        for region in regions:

            deps = _coerce_dependencies(region, upstreams)

            for expression in expressions:
                checks._check_expression_type(asset_fn, expression["expression_type"])

                name = f"{asset_fn.__name__}_{expression['expression_type']}"
                resource_key = (
                    f"{region.replace('-', '_')}__{METRIC_ENTITIES_DATABASE}__{name}"
                )

                _description = expression["metadata"]["comment"]
                checks._check_asset_description(asset_fn.__name__, _description)

                metadata["measure"] = expression["expression"]
                metadata["expression"] = {
                    "expression_type": expression["expression_type"],
                    "expression": expression["expression"],
                    "metadata": expression["metadata"],
                }
                metadata["description"] = _description

                @asset(
                    name=name,
                    key_prefix=[region, METRIC_ENTITIES_DATABASE],
                    deps=deps,
                    compute_kind=f"{region[0:2]}_metric",
                    group_name=f"metric_repo_{region[0:2]}",
                    output_required=False,
                    description=_description,
                    metadata=metadata,
                    owners=_owners,
                    required_resource_keys={resource_key},
                    resource_defs={
                        resource_key: ConfigurableDatabricksStepLauncher(
                            region=region,
                            max_workers=1,
                            instance_pool_type=GENERAL_PURPOSE_INSTANCE_POOL_KEY,
                            spark_version=DEFAULT_SPARK_VERSION,
                            timeout_seconds=3600,
                            spark_conf_overrides={
                                "spark.databricks.sql.initial.catalog.name": "default",
                            },
                        )
                    },
                )
                def _asset(context: AssetExecutionContext):

                    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

                    metric_name = context.asset_key.path[-1]
                    metric_expression = context.assets_def.metadata_by_key[
                        context.asset_key
                    ]["expression"]
                    metric_description = metric_expression["metadata"]["comment"]

                    definition = asset_fn(context)
                    metadata["definition"] = definition
                    schema = spark.sql(definition).schema.jsonValue()["fields"]
                    schema = _update_schema(schema, dimensions)

                    _create_metric_entity(
                        context=context,
                        spark=spark,
                        name=metric_name,
                        definition=definition,
                        description=metric_description,
                        schema=schema,
                    )
                    _create_metric_function(
                        context,
                        spark,
                        metric_name,
                        definition,
                        metric_expression,
                        schema,
                    )

                assets.append(_asset)
        return assets

    return _wrapper
