from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""A dataset containing a daily summary of trip details and associated safety events.""",
        row_meaning="""Trip details and any associated safety events""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {
            "name": "date",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The calendar date in `YYYY-mm-dd` format corresponding to the trip"
            },
        },
        {
            "name": "org_id",
            "type": "long",
            "nullable": False,
            "metadata": {"comment": "The Internal ID for the customer`s Samsara org"},
        },
        {
            "name": "sam_number",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "This is the internal Samsara Customer account ID."
            },
        },
        {
            "name": "account_size_segment",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "A bucketing of customer accounts by sizes - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "vg_device_id",
            "type": "long",
            "nullable": True,
            "metadata": {
                "comment": "The ID of the customer device that the trip is linked to"
            },
        },
        {
            "name": "product_id",
            "type": "long",
            "nullable": True,
            "metadata": {
                "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
            },
        },
        {
            "name": "device_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
            },
        },
        {
            "name": "is_driver_assigned",
            "type": "integer",
            "nullable": True,
            "metadata": {"comment": "Whether a driver has been assigned or not"},
        },
        {
            "name": "driver_assignment_source",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "The source used for assigning a driver to this trip. E.g., HOS, Tachograph"
            },
        },
        {
            "name": "camera_device_id",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Device ID for camera"},
        },
        {
            "name": "camera_product_id",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Product ID for camera"},
        },
        {
            "name": "camera_product_name",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "Product name for camera"},
        },
        {
            "name": "start_time_ms",
            "type": "long",
            "nullable": False,
            "metadata": {
                "comment": "The start timestamp of the trip in epoch milliseconds"
            },
        },
        {
            "name": "end_time_ms",
            "type": "long",
            "nullable": True,
            "metadata": {
                "comment": "The end timestamp of the trip in epoch milliseconds"
            },
        },
        {
            "name": "start_country",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "Start country of trip"},
        },
        {
            "name": "end_country",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "End country of trip"},
        },
        {
            "name": "duration_mins",
            "type": "double",
            "nullable": True,
            "metadata": {"comment": "Duration (in minutes) of the trip"},
        },
        {
            "name": "distance_miles",
            "type": "double",
            "nullable": True,
            "metadata": {"comment": "Distance (in miles) of the trip"},
        },
        {
            "name": "trip_type",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Indicates the type of trip: location_based, engine_based"
            },
        },
        {
            "name": "num_safety_events",
            "type": "integer",
            "nullable": True,
            "metadata": {"comment": "Number of safety events associated with the trip"},
        },
        {
            "name": "region",
            "type": "string",
            "nullable": False,
            "metadata": {"comment": "AWS cloud region where record comes from"},
        },
    ],
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_safety.fct_safety_events",
        "us-west-2|eu-west-1|ca-central-1:datamodel_telematics.fct_trips",
    ],
    primary_keys=[
        "date",
        "org_id",
        "vg_device_id",
        "camera_device_id",
        "start_time_ms",
        "end_time_ms",
    ],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_trip_details_global"),
        NonNullDQCheck(
            name="dq_non_null_trip_details_global",
            non_null_columns=["date", "org_id", "region", "start_time_ms"],
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_trip_details_global",
            primary_keys=[
                "date",
                "org_id",
                "vg_device_id",
                "camera_device_id",
                "start_time_ms",
                "end_time_ms",
            ],
            block_before_write=True,
        ),
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def trip_details_global(context: AssetExecutionContext) -> str:

    query = """--sql
    WITH
    us_safety_events_per_trip AS (
        SELECT
            s.date,
            s.org_id,
            s.device_id,
            s.trip_start_ms,
            array_size(collect_set(s.event_ms)) as num_safety_events
        FROM datamodel_safety.fct_safety_events s
        INNER JOIN datamodel_core.dim_organizations AS orgs -- this join is to find non-internal records
        ON orgs.org_id = s.org_id
        AND s.date = orgs.date
        WHERE s.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        AND s.release_stage in (2, 3, 4) -- open beta, closed beta, and GA events
        GROUP BY 1,2,3,4
    ),
    us_trips AS (
        SELECT ft.date,
                ft.org_id,
                orgs.sam_number,
                orgs.account_size_segment,
                ft.device_id AS vg_device_id,
                dd.product_id AS product_id,
                dd.product_name AS device_type,
                CASE
                WHEN ft.driver_id = 0 OR ft.driver_id IS NULL THEN 0
                ELSE 1
                END AS is_driver_assigned,
                driver_assignment_source,
                dd.associated_devices.camera_device_id,
                dd.associated_devices.camera_product_id,
                cm_prod.name AS camera_product_name,
                ft.start_time_ms,
                ft.end_time_ms,
                ft.start_country,
                ft.end_country,
                ft.duration_mins,
                ft.distance_miles,
                ft.trip_type
        FROM datamodel_telematics.fct_trips ft
        INNER JOIN datamodel_core.dim_organizations AS orgs -- this join is to find non-internal records
            ON orgs.org_id = ft.org_id
            AND ft.date = orgs.date
        INNER JOIN datamodel_core.dim_devices AS dd -- this join is to find non-internal records
            ON dd.device_id = ft.device_id
            AND ft.date = dd.date
        LEFT JOIN definitions.products cm_prod
            ON dd.associated_devices.camera_product_id = cm_prod.product_id
        WHERE TRUE -- arbitrary for readability
            AND ft.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- want to pull relavant dates
            AND orgs.internal_type = 0 -- non-internal orgs
            AND orgs.account_first_purchase_date IS NOT NULL -- has at lease one purchase
            AND dd.device_type LIKE 'VG%' -- vg trips
        GROUP BY ALL -- Like SELECT DISTINCT, but faster
    ),
    us_trip_details AS (
        SELECT
                td.date,
                td.org_id,
                td.sam_number,
                td.account_size_segment,
                td.vg_device_id,
                td.product_id,
                td.device_type,
                td.is_driver_assigned,
                td.driver_assignment_source,
                td.camera_device_id,
                td.camera_product_id,
                td.camera_product_name,
                td.start_time_ms,
                td.end_time_ms,
                td.start_country,
                td.end_country,
                td.duration_mins,
                td.distance_miles,
                td.trip_type,
                s.num_safety_events
        FROM us_trips td
        LEFT JOIN us_safety_events_per_trip s
        ON td.date = s.date
            AND td.org_id = s.org_id
            AND td.start_time_ms = s.trip_start_ms
            AND td.vg_device_id = s.device_id
    ),
    eu_safety_events_per_trip AS (
        SELECT
            s.date,
            s.org_id,
            s.device_id,
            s.trip_start_ms,
            array_size(collect_set(s.event_ms)) as num_safety_events
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_safety.db/fct_safety_events` s
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` AS orgs -- this join is to find non-internal records
        ON orgs.org_id = s.org_id
        AND s.date = orgs.date
        WHERE s.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        AND s.release_stage in (2, 3, 4) -- open beta, closed beta, and GA events
        GROUP BY 1,2,3,4
    ),
    eu_trips AS (
        SELECT ft.date,
                ft.org_id,
                orgs.sam_number,
                orgs.account_size_segment,
                ft.device_id AS vg_device_id,
                dd.product_id AS product_id,
                dd.product_name AS device_type,
                CASE
                WHEN ft.driver_id = 0 OR ft.driver_id IS NULL THEN 0
                ELSE 1
                END AS is_driver_assigned,
                driver_assignment_source,
                dd.associated_devices.camera_device_id,
                dd.associated_devices.camera_product_id,
                cm_prod.name AS camera_product_name,
                ft.start_time_ms,
                ft.end_time_ms,
                ft.start_country,
                ft.end_country,
                ft.duration_mins,
                ft.distance_miles,
                ft.trip_type
        FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_telematics.db/fct_trips` ft
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` AS orgs -- this join is to find non-internal records
            ON orgs.org_id = ft.org_id
            AND ft.date = orgs.date
        INNER JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_devices` AS dd -- this join is to find non-internal records
            ON dd.device_id = ft.device_id
            AND ft.date = dd.date
        LEFT JOIN definitions.products cm_prod
            ON dd.associated_devices.camera_product_id = cm_prod.product_id
        WHERE TRUE -- arbitrary for readability
            AND ft.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- want to pull relavant dates
            AND orgs.internal_type = 0 -- non-internal orgs
            AND orgs.account_first_purchase_date IS NOT NULL -- has at lease one purchase
            AND dd.device_type LIKE 'VG%' -- vg trips
        GROUP BY ALL -- Like SELECT DISTINCT, but faster
    ),
    eu_trip_details AS (
        SELECT
            td.date,
            td.org_id,
            td.sam_number,
            td.account_size_segment,
            td.vg_device_id,
            td.product_id,
            td.device_type,
            td.is_driver_assigned,
            td.driver_assignment_source,
            td.camera_device_id,
            td.camera_product_id,
            td.camera_product_name,
            td.start_time_ms,
            td.end_time_ms,
            td.start_country,
            td.end_country,
            td.duration_mins,
            td.distance_miles,
            td.trip_type,
            s.num_safety_events
        FROM eu_trips td
        LEFT JOIN eu_safety_events_per_trip s
        ON td.date = s.date
            AND td.org_id = s.org_id
            AND td.start_time_ms = s.trip_start_ms
            AND td.vg_device_id = s.device_id
    ),
    ca_safety_events_per_trip AS (
        SELECT
            s.date,
            s.org_id,
            s.device_id,
            s.trip_start_ms,
            array_size(collect_set(s.event_ms)) as num_safety_events
        FROM data_tools_delta_share_ca.datamodel_safety.fct_safety_events s
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations AS orgs -- this join is to find non-internal records
        ON orgs.org_id = s.org_id
        AND s.date = orgs.date
        WHERE s.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- grab relavant dates
        AND s.release_stage in (2, 3, 4) -- open beta, closed beta, and GA events
        GROUP BY 1,2,3,4
    ),
    ca_trips AS (
        SELECT ft.date,
                ft.org_id,
                orgs.sam_number,
                orgs.account_size_segment,
                ft.device_id AS vg_device_id,
                dd.product_id AS product_id,
                dd.product_name AS device_type,
                CASE
                WHEN ft.driver_id = 0 OR ft.driver_id IS NULL THEN 0
                ELSE 1
                END AS is_driver_assigned,
                driver_assignment_source,
                dd.associated_devices.camera_device_id,
                dd.associated_devices.camera_product_id,
                cm_prod.name AS camera_product_name,
                ft.start_time_ms,
                ft.end_time_ms,
                ft.start_country,
                ft.end_country,
                ft.duration_mins,
                ft.distance_miles,
                ft.trip_type
        FROM data_tools_delta_share_ca.datamodel_telematics.fct_trips ft
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations AS orgs -- this join is to find non-internal records
            ON orgs.org_id = ft.org_id
            AND ft.date = orgs.date
        INNER JOIN data_tools_delta_share_ca.datamodel_core.dim_devices AS dd -- this join is to find non-internal records
            ON dd.device_id = ft.device_id
            AND ft.date = dd.date
        LEFT JOIN definitions.products cm_prod
            ON dd.associated_devices.camera_product_id = cm_prod.product_id
        WHERE TRUE -- arbitrary for readability
            AND ft.date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}' -- want to pull relavant dates
            AND orgs.internal_type = 0 -- non-internal orgs
            AND orgs.account_first_purchase_date IS NOT NULL -- has at lease one purchase
            AND dd.device_type LIKE 'VG%' -- vg trips
        GROUP BY ALL -- Like SELECT DISTINCT, but faster
    ),
    ca_trip_details AS (
        SELECT
            td.date,
            td.org_id,
            td.sam_number,
            td.account_size_segment,
            td.vg_device_id,
            td.product_id,
            td.device_type,
            td.is_driver_assigned,
            td.driver_assignment_source,
            td.camera_device_id,
            td.camera_product_id,
            td.camera_product_name,
            td.start_time_ms,
            td.end_time_ms,
            td.start_country,
            td.end_country,
            td.duration_mins,
            td.distance_miles,
            td.trip_type,
            s.num_safety_events
        FROM ca_trips td
        LEFT JOIN ca_safety_events_per_trip s
        ON td.date = s.date
            AND td.org_id = s.org_id
            AND td.start_time_ms = s.trip_start_ms
            AND td.vg_device_id = s.device_id
    )
    SELECT DISTINCT
        date,
        org_id,
        sam_number,
        account_size_segment,
        vg_device_id,
        product_id,
        device_type,
        is_driver_assigned,
        driver_assignment_source,
        camera_device_id,
        camera_product_id,
        camera_product_name,
        start_time_ms,
        end_time_ms,
        start_country,
        end_country,
        duration_mins,
        distance_miles,
        trip_type,
        num_safety_events,
        'us-west-2' AS region
    FROM
        us_trip_details

    UNION

    SELECT DISTINCT
        date,
        org_id,
        sam_number,
        account_size_segment,
        vg_device_id,
        product_id,
        device_type,
        is_driver_assigned,
        driver_assignment_source,
        camera_device_id,
        camera_product_id,
        camera_product_name,
        start_time_ms,
        end_time_ms,
        start_country,
        end_country,
        duration_mins,
        distance_miles,
        trip_type,
        num_safety_events,
        'eu-west-1' AS region
    FROM eu_trip_details

    UNION

    SELECT DISTINCT
        date,
        org_id,
        sam_number,
        account_size_segment,
        vg_device_id,
        product_id,
        device_type,
        is_driver_assigned,
        driver_assignment_source,
        camera_device_id,
        camera_product_id,
        camera_product_name,
        start_time_ms,
        end_time_ms,
        start_country,
        end_country,
        duration_mins,
        distance_miles,
        trip_type,
        num_safety_events,
        'ca-central-1' AS region
    FROM ca_trip_details
    --endsql"""
    return query
