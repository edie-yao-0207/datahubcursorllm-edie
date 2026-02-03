from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    GENERAL_PURPOSE_INSTANCE_POOL_KEY,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from dataweb.userpkgs.query import create_run_config_overrides

PRIMARY_KEYS = ["org_id", "uuid"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier for the coachable item"
        },
    },
    {
        "name": "source_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Source identifier for the coachable item"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "ID of the driver associated with the coachable item"
        },
    },
    {
        "name": "coachable_item_backing_type_enum",
        "type": "short",
        "nullable": False,
        "metadata": {
            "comment": "The product category that triggered the coachable item. Also referred to as CoachableItemBackingType in backend."
        },
    },
    {
        "name": "coachable_item_backing_type_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The name of the coachable item backing type"
        },
    },
    {
        "name": "coachable_item_type_enum",
        "type": "short",
        "nullable": False,
        "metadata": {
            "comment": "The type of behavior that triggered the coachable item. As new harsh events get added, new coachable item types will need to be added here."
        },
    },
    {
        "name": "coachable_item_type_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The name of the coachable item type"
        },
    },
    {
        "name": "coachable_item_status_enum",
        "type": "short",
        "nullable": False,
        "metadata": {
            "comment": "Current status of the coachable item. Also referred to as CoachableItemStatus in backend."
        },
    },
    {
        "name": "coachable_item_status_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The name of the coachable item status"
        },
    },
    {
        "name": "coaching_session_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "UUID of the coaching session associated with the item"
        },
    },
    {
        "name": "coaching_session_behavior_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "UUID of the coaching session behavior"
        },
    },
    {
        "name": "aggregate_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Aggregate UUID used for group coaching"
        },
    },
    {
        "name": "aggregation_key",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Aggregation key used for group coaching"
        },
    },
    {
        "name": "disputed_reason_enum",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "The reason for disputing the coachable item. Also referred to as CoachableItemDisputeReason in backend."
        },
    },
    {
        "name": "disputed_time",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp when the coachable item was disputed"
        },
    },
    {
        "name": "created_at_datetime",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp when the coachable item was created"
        },
    },
    {
        "name": "updated_at_datetime",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp when the coachable item was last updated"
        },
    },
    {
        "name": "due_at_datetime",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp when the coachable item is due"
        },
    },
]

QUERY = """--sql
    SELECT
        ci.date
        , ci.org_id
        , ci.uuid
        , ci.source_id
        , ci.driver_id
        , ci.item_type AS coachable_item_backing_type_enum
        , cib.coachable_item_backing_type AS coachable_item_backing_type_name
        , ci.coachable_item_type AS coachable_item_type_enum
        , cit.coachable_item_behavior AS coachable_item_type_name
        , ci.coaching_state AS coachable_item_status_enum
        , cis.name AS coachable_item_status_name
        , ci.coaching_session_uuid
        , ci.coaching_session_behavior_uuid
        , ci.aggregate_uuid
        , ci.aggregation_key
        , ci.disputed_reason AS disputed_reason_enum
        , ci.disputed_time
        , ci.created_at AS created_at_datetime
        , ci.updated_at AS updated_at_datetime
        , ci.due_date AS due_at_datetime
    FROM coachingdb_shards.coachable_item ci
    LEFT JOIN definitions.coachable_item_backing_type cib ON ci.item_type = cib.id
    LEFT JOIN definitions.coachable_item_type cit ON ci.coachable_item_type = cit.id
    LEFT JOIN definitions.coachable_item_status_enums cis ON ci.coaching_state = cis.enum
    WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
--endsql"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Coachable item data containing information about coaching items, their states, and associated metadata. This table tracks items that can be used for driver coaching purposes.",
        row_meaning="A coachable item that can be used for driver coaching",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2020-01-01"),
    upstreams=[
        "definitions.coachable_item_backing_type",
        "definitions.coachable_item_type",
        "definitions.coachable_item_status_enums",
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_coachable_items"),
        NonNullDQCheck(
            name="dq_non_null_fct_coachable_items",
            # The _name columns are included here to ensure that the definitions table
            # is not missing any enum mappings.
            non_null_columns=[
                "date",
                "org_id",
                "uuid",
                "source_id",
                "coachable_item_backing_type_enum",
                "coachable_item_backing_type_name",
                "coachable_item_type_enum",
                "coachable_item_type_name",
                "coachable_item_status_enum",
                "coachable_item_status_name",
                "created_at_datetime",
                "updated_at_datetime",
            ],
            block_before_write=True
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_coachable_items",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True
        ),
    ],
    backfill_batch_size=7,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=GENERAL_PURPOSE_INSTANCE_POOL_KEY,
        max_workers=4,
    ),
)
def fct_coachable_items(context: AssetExecutionContext) -> str:
    context.log.info("Updating fct_coachable_items")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
