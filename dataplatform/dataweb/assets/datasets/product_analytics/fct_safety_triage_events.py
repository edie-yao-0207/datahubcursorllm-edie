from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
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

PRIMARY_KEYS = [
    "uuid"
]

SCHEMA = [
    {
        "name": "uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier for the safety triage event"
        },
    },
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DEVICE_ID
        },
    },
    {
        "name": "start_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Start timestamp of the event in milliseconds"
        },
    },
    {
        "name": "end_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "End timestamp of the event in milliseconds"
        },
    },
    {
        "name": "trip_start_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Trip start timestamp in milliseconds"
        },
    },
    {
        "name": "trigger_label_enum",
        "type": "short",
        "nullable": False,
        "metadata": {
            "comment": "The enum for the automatically-detected behavior label for the safety event (i.e., the original behavior label that triggered the event)"
        },
    },
    {
        "name": "trigger_label_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The name of the original behavior label (e.g., 'Speeding', 'HarshTurn', 'MobileUsage', 'ForwardCollisionWarning', etc.)"
        },
    },
    {
        "name": "behavior_labels_array",
        "type": {'type': 'array', 'elementType': 'integer', 'containsNull': True},
        "nullable": False,
        "metadata": {
            "comment": "An array that contains all the behavior label enums that are currently applied to the event. This could contain the original trigger label (or not, if the original trigger label was removed) as well as any additional labels that were added after the event was created."
        },
    },
    {
        "name": "behavior_labels_names_array",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": False,
        "metadata": {
            "comment": "An array of behavior label type names corresponding to the behavior_labels_array values, joined with definitions.behavior_label_type_enums"
        },
    },
    {
        "name": "triage_state_enum",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "Numeric enum value for the triage state"
        },
    },
    {
        "name": "triage_state_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Human-readable name for the triage state (e.g., 'NeedsReview', 'Reviewed', 'InCoaching', etc.)"
        },
    },
    {
        "name": "coaching_state",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Parsed integer value from the coaching state"
        },
    },
    {
        "name": "coaching_state_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Human-readable name for the coaching state (e.g., 'ITEM_DISMISSED', 'ITEM_NEEDS_COACHING', 'ITEM_COACHED', etc.)"
        },
    },
    {
        "name": "is_starred",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the event has been starred"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "ID of the driver associated with the event"
        },
    },
    {
        "name": "created_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp when the event was created"
        },
    },
    {
        "name": "updated_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp when the event was last updated"
        },
    },
    {
        "name": "release_stage",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "Release stage of the event"
        },
    },
    {
        "name": "priority",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Priority level of the event"
        },
    },
    {
        "name": "version",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "Version of the event"
        },
    },
    {
        "name": "activity_enums_array",
        "type": {'type': 'array', 'elementType': 'integer', 'containsNull': True},
        "nullable": True,
        "metadata": {
            "comment": "An array that contains all the activities that occurred for the event, as enum values"
        },
    },
    {
        "name": "activity_names_array",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": True,
        "metadata": {
            "comment": "An array that contains all the activities that occurred for the event, as names"
        },
    },
    {
        "name": "was_event_viewed",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether or not there was a view activity for the event"
        },
    },
]

QUERY = """--sql
    WITH base_query AS (
        SELECT
            te.uuid
            , CAST(te.date AS STRING) AS date
            , te.org_id
            , te.device_id
            , te.start_ms
            , te.end_ms
            , te.trip_start_ms
            , te.trigger_label AS trigger_label_enum
            , bl.behavior_label_type AS trigger_label_name
            , TRANSFORM(
              FILTER(SPLIT(behavior_labels, '\\\|'), x -> x != ''),
              x -> CAST(x AS INT)
            ) AS behavior_labels_array
            , te.triage_state AS triage_state_enum
            , COALESCE(ts.triage_state, 'Unknown') AS triage_state_name
            , CAST(regexp_extract(coaching_state, '"([^"]*)"', 1) AS INT) AS coaching_state
            , CAST(te.is_starred AS BOOLEAN) AS is_starred
            , te.driver_id
            , te.created_at_ms
            , te.updated_at_ms
            , te.release_stage
            , te.priority
            , CAST(te.version AS SMALLINT) AS version
        FROM safetyeventtriagedb_shards.triage_events_v2 te
        LEFT JOIN definitions.triage_state ts ON te.triage_state = ts.triage_state_id
        LEFT JOIN definitions.behavior_label_type_enums bl ON te.trigger_label = bl.enum
        WHERE te.release_stage != 1
            AND te.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    )
    , exploded_labels AS (
        SELECT
            bq.*
            , label_enum
        FROM base_query bq
        LATERAL VIEW OUTER EXPLODE(behavior_labels_array) t AS label_enum
    )
    , joined_labels AS (
        SELECT
            el.*
            , bv.behavior_label_type
            , ci.name AS coaching_state_name
        FROM exploded_labels el
        LEFT JOIN definitions.behavior_label_type_enums bv ON el.label_enum = bv.enum
        LEFT JOIN definitions.coachable_item_status_enums ci ON el.coaching_state = ci.enum
    )
    -- Groups by all columns except behavior_labels_names_array
    , grouped_labels AS (
        SELECT
            uuid
            , date
            , org_id
            , device_id
            , start_ms
            , end_ms
            , trip_start_ms
            , trigger_label_enum
            , trigger_label_name
            , behavior_labels_array
            , COLLECT_LIST(behavior_label_type) AS behavior_labels_names_array
            , triage_state_enum
            , triage_state_name
            , coaching_state
            , coaching_state_name
            , is_starred
            , driver_id
            , created_at_ms
            , updated_at_ms
            , release_stage
            , priority
            , version
        FROM joined_labels
        GROUP BY
            uuid
            , date
            , org_id
            , device_id
            , start_ms
            , end_ms
            , trip_start_ms
            , trigger_label_enum
            , trigger_label_name
            , behavior_labels_array
            , triage_state_enum
            , triage_state_name
            , coaching_state
            , coaching_state_name
            , is_starred
            , driver_id
            , created_at_ms
            , updated_at_ms
            , release_stage
            , priority
            , version
    )
    , activity_events AS (
        SELECT
            CAST(date AS string) AS date
            , ae.org_id
            , ae.device_id
            , ae.event_ms
            , ae.created_at
            , ae.activity_type AS activity_enum
            , es.activity_type AS activity_name
        FROM safetydb_shards.activity_events ae
        LEFT JOIN definitions.safety_activity_type_enums es ON ae.activity_type = es.enum
        WHERE ae.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    )
    , activity_events_grouped AS (
        SELECT
            date
            , org_id
            , device_id
            , event_ms
            , COLLECT_SET(CAST(activity_enum AS INT)) AS activity_enums_array
            , COLLECT_SET(activity_name) AS activity_names_array
        FROM activity_events
        GROUP BY 1, 2, 3, 4
    )
    SELECT
        gl.*
        , ag.activity_enums_array
        , ag.activity_names_array
        , COALESCE(ARRAY_CONTAINS(ag.activity_names_array, 'ViewedByActivityType'), FALSE) AS was_event_viewed
    FROM grouped_labels gl
    LEFT JOIN activity_events_grouped ag ON gl.date = ag.date
        AND gl.org_id = ag.org_id
        AND gl.device_id = ag.device_id
        AND gl.start_ms = ag.event_ms

--endsql"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Safety triage events data containing information about safety events, their triage status, and associated metadata. Note: This table is 1:1 with events in the new Aggregated Safety Inbox, not the legacy Safety Inbox.",
        row_meaning="A safety event that is visible to the customer in the Aggregated Inbox",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2020-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_safety_triage_events"),
        NonNullDQCheck(
            name="dq_non_null_fct_safety_triage_events",
            non_null_columns=["uuid", "date", "org_id", "device_id", "start_ms", "trigger_label_enum", "trigger_label_name", "behavior_labels_array", "behavior_labels_names_array"],
            block_before_write=True
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_safety_triage_events",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True
        ),
    ],
    backfill_batch_size=7,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=16,
    ),
)
def fct_safety_triage_events(context: AssetExecutionContext) -> str:
    context.log.info("Updating fct_safety_triage_events")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
