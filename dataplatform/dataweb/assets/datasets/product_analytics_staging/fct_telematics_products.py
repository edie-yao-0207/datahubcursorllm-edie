from enum import Enum

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.metric import StrEnum
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import Definitions, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)


class TableDimensions(StrEnum):
    PRODUCT_ID = "product_id"
    PRODUCT_GROUP_ID = "product_group_id"
    CABLE_ID = "cable_id"
    HAS_MODI_OR_OVERRIDE = "has_modi_or_override"


SCHEMA = columns_to_schema(
    Column(
        name=TableDimensions.PRODUCT_ID,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The internal Samsara product identifier. One value is expected per hardware product."
        ),
    ),
    Column(
        name=TableDimensions.PRODUCT_GROUP_ID,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The internal Samsara product group identifier. One value is expected per organization (Telematics, Safety)."
        ),
    ),
    Column(
        name=TableDimensions.CABLE_ID,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The internal Samsara cable SKU identifier. One value is expected per cable type."
        ),
    ),
    ColumnType.PRODUCT_NAME,
    ColumnType.CABLE_NAME,
    Column(
        name=TableDimensions.HAS_MODI_OR_OVERRIDE,
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="True if the cable requires a modi (port extender) for a given product and cable installation combination."
        ),
    ),
    ColumnType.DIAGNOSTICS_CAPABLE,
)

PRIMARY_KEYS = [
    TableDimensions.PRODUCT_ID,
    TableDimensions.PRODUCT_GROUP_ID,
    TableDimensions.CABLE_ID,
]

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

SELECT
    products.product_id
    , CAST(products.product_group_id AS LONG) as product_group_id
    , cable_type_mappings.cable_id
    , products.name as product_name
    , cable_type_mappings.type as cable_name
    , cable_type_mappings.has_modi_or_override
    , cable_type_mappings.diagnostics_capable

FROM
    definitions.products

INNER JOIN
    definitions.cable_type_mappings
    ON products.product_id = cable_type_mappings.product_id

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Combinations of VGs and possible diagnostic cables installed.",
        row_meaning="Product and diagnostic cable combinations.",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    upstreams=[
        AnyUpstream(Definitions.PRODUCTS),
        AnyUpstream(Definitions.CABLE_TYPE_MAPPINGS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_PRODUCTS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
)
def fct_telematics_products(context: AssetExecutionContext) -> str:
    return QUERY
