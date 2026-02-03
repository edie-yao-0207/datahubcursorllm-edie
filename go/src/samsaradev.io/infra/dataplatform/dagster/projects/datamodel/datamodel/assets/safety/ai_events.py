from datetime import datetime

from dagster import AssetIn, BackfillPolicy, DailyPartitionsDefinition, asset
from pyspark.sql import DataFrame, SparkSession

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    build_table_description,
)
from ..source_assets import (
    clouddb,
    dataprep,
    definitions,
    kinesisstats,
    productsdb,
    safetydb_shards,
)

safetydb_harsh_event_surveys = safetydb_shards.safetydb_shards_harsh_event_surveys[
    AWSRegions.US_WEST_2.value
]
safetydb_safety_events = safetydb_shards.safetydb_shards_safety_events[
    AWSRegions.US_WEST_2.value
]
kinesisstats_osdaccelerometer = kinesisstats.kinesisstats_osdaccelerometer[
    AWSRegions.US_WEST_2.value
]
dataprep_device_builds = dataprep.dataprep_device_builds[AWSRegions.US_WEST_2.value]
dataprep_active_devices = dataprep.dataprep_active_devices[AWSRegions.US_WEST_2.value]
clouddb_organizations = clouddb.clouddb_organizations[AWSRegions.US_WEST_2.value]
productsdb_devices = productsdb.productsdb_devices[AWSRegions.US_WEST_2.value]
definitions_products = definitions.definitions_products[AWSRegions.US_WEST_2.value]
definitions_harsh_accel_type_enums = definitions.definitions_harsh_accel_type_enums[
    AWSRegions.US_WEST_2.value
]


static_partition_def = DailyPartitionsDefinition(
    start_date=datetime(2023, 7, 26), end_offset=1
)

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=7)
pipeline_group_name = "safety_ai_events"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=static_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    backfill_policy=BACKFILL_POLICY,
    regions=[AWSRegions.US_WEST_2.value],
)


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    description=build_table_description(
        table_desc="""This table contains firmware events from VGs and CMs""",
        row_meaning="""Each row contains a firmware event from a VG or CM device""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    owners=["team:DataEngineering"],
    key_prefix=[AWSRegions.US_WEST_2.value, "dataengineering"],
    metadata={
        "description": build_table_description(
            table_desc="""This table contains firmware events from VGs and CMs""",
            row_meaning="""Each row contains a firmware event from a VG or CM device""",
            related_table_info={},
            table_type=TableType.TRANSACTIONAL_FACT,
            freshness_slo_updated_by="9am PST",
        ),
        "database": Database.DATAENGINEERING,
        "owners": ["team:DataEngineering"],
        "schema": [
            {
                "name": "date",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
                },
            },
            {
                "name": "time",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "Time at which the event happened"},
            },
            {
                "name": "org_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": org_id_default_description},
            },
            {
                "name": "object_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "ID of object involved in event"},
            },
            {
                "name": "event_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "ID of the safety event"},
            },
            {
                "name": "device_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": device_id_default_description},
            },
            {
                "name": "vg_product_name",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Product name for the VG"},
            },
            {
                "name": "camera_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "ID of camera capturing event"},
            },
            {
                "name": "cm_product_name",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Product name of device"},
            },
            {
                "name": "vg_build",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "The latest build version of the device on 'date'"
                },
            },
            {
                "name": "cm_build",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "The latest build version of the device on 'date'"
                },
            },
            {
                "name": "distance_traveled",
                "type": "double",
                "nullable": True,
                "metadata": {
                    "comment": "The total distance (in meters) that the device has travelled"
                },
            },
            {
                "name": "customer_phase",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Phase the customer was in"},
            },
            {
                "name": "customer_type",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Type of customer involved in the event"},
            },
            {
                "name": "harsh_event_type",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Harsh event that spawned the incident"},
            },
        ],
        "write_mode": "merge",
        "primary_keys": ["date", "time", "org_id", "object_id", "event_id"],
    },
    ins={
        "devices": AssetIn(key=productsdb_devices.key),
        "products": AssetIn(key=definitions_products.key),
        "device_builds": AssetIn(
            key=dataprep_device_builds.key,
            metadata=dict(dataprep_device_builds.metadata, **{"lookback_period": 7}),
        ),
        "active_devices": AssetIn(
            key=dataprep_active_devices.key,
            metadata=dict(dataprep_active_devices.metadata, **{"lookback_period": 7}),
        ),
        "organizations": AssetIn(key=clouddb_organizations.key),
        "harsh_accel_type_enums": AssetIn(key=definitions_harsh_accel_type_enums.key),
        "osdaccelerometer": AssetIn(
            key=kinesisstats_osdaccelerometer.key,
            metadata=dict(
                kinesisstats_osdaccelerometer.metadata, **{"lookback_period": 7}
            ),
        ),
    },
    partitions_def=static_partition_def,
    group_name=pipeline_group_name,
    backfill_policy=BACKFILL_POLICY,
)
def firmware_events(
    devices: DataFrame,
    products: DataFrame,
    device_builds: DataFrame,
    active_devices: DataFrame,
    organizations: DataFrame,
    harsh_accel_type_enums: DataFrame,
    osdaccelerometer: DataFrame,
) -> DataFrame:
    devices.createOrReplaceTempView("productsdb_devices")
    products.createOrReplaceTempView("definitions_products")
    device_builds.createOrReplaceTempView("dataprep_device_builds")
    active_devices.createOrReplaceTempView("dataprep_active_devices")
    organizations.createOrReplaceTempView("clouddb_organizations")
    harsh_accel_type_enums.createOrReplaceTempView("definitions_harsh_accel_type_enums")
    osdaccelerometer.createOrReplaceTempView("kinesisstats_history_osdaccelerometer")

    query = """--sql
    WITH devices_vgs AS (
    -- Devices in productsdb.devices can be either customer devices or cm's
    SELECT
        dv.org_id
        ,dv.id AS device_id -- this is the customer device id (not vehicle gateway (vg) id)
        ,dc.id AS camera_id
        ,pv.name AS vg_product_name
        ,pc.name AS cm_product_name
    FROM productsdb_devices dv
    -- Not all vg's have associated cm's, hence the left and not inner join
    LEFT JOIN productsdb_devices dc ON upper(replace(replace(dv.camera_serial, '-', ''), ' ', '')) = dc.serial
    JOIN definitions_products pv ON dv.product_id = pv.product_id
    LEFT JOIN definitions_products pc ON dc.product_id = pc.product_id
    WHERE upper(pv.name) RLIKE 'VG'
    ),

    devices_cms AS (
    SELECT
        dv.org_id -- multiple vg's can have the same camera_serial
        ,dc.id AS camera_id -- for cm's in productsdb.devices, id represents the cm's id (not the customer device_id)
        ,dv.id AS device_id -- this is the customer device_id (not vehicle gateway (vg) id)
        ,pc.name AS cm_product_name
        ,pv.name AS vg_product_name
    FROM productsdb_devices dc
    -- A small number of cm's don't have associated vg's, hence the left and not inner join
    LEFT JOIN productsdb_devices dv ON dc.serial = upper(replace(replace(dv.camera_serial, '-', ''), ' ', ''))
    JOIN definitions_products pc ON dc.product_id = pc.product_id
    LEFT JOIN definitions_products pv ON dv.product_id = pv.product_id
    WHERE upper(pc.name) RLIKE 'CM'
        AND upper(pv.name) RLIKE 'VG' -- some cm's can be paired with ag's and other non-vg devices
    ),

    firmware_events_vgs AS (
    -- The inner join - oa.object_id = devices_vgs.device_id - will result in only those events where the
    -- object_id represents a customer device_id.
    SELECT
        oa.date
        ,oa.time
        ,oa.org_id
        ,oa.object_id
        ,oa.value.proto_value.accelerometer_event.event_id
        ,dv.device_id
        ,dv.vg_product_name
        ,dv.camera_id
        ,dv.cm_product_name
        ,db_vg.latest_build_on_day AS vg_build
        ,db_cm.latest_build_on_day AS cm_build
        ,ad.total_distance AS distance_traveled
        ,CASE WHEN o.release_type_enum = 0 THEN "Phase 1"
        WHEN o.release_type_enum = 1 THEN "Phase 2"
        WHEN o.release_type_enum = 2 THEN "Early Adopter"
        END AS customer_phase
        ,CASE WHEN o.internal_type = 0 THEN "Customer Org"
        WHEN o.internal_type = 1 THEN "Internal Org"
        WHEN o.internal_type = 2 THEN "Lab Org"
        WHEN o.internal_type = 3 THEN "Developer Portal Test Org"
        WHEN o.internal_type = 4 THEN "Developer Portal Dev Org"
        END AS customer_type
        ,ha.event_type AS harsh_event_type
    FROM kinesisstats_history_osdaccelerometer oa -- use _history in case of backfill
    -- **Note that devices_vgs.device_id will always be a customer device_id (due to product filter in devices_vgs).**
    JOIN devices_vgs dv ON oa.object_id = dv.device_id
        AND oa.org_id = dv.org_id
    LEFT JOIN dataprep_device_builds db_vg ON dv.device_id = db_vg.device_id
        AND oa.`date` = db_vg.`date`
        AND oa.org_id = db_vg.org_id
    LEFT JOIN dataprep_device_builds db_cm ON dv.camera_id = db_cm.device_id
        AND oa.`date` = db_cm.`date`
        AND oa.org_id = db_cm.org_id
    LEFT JOIN dataprep_active_devices ad ON dv.device_id = ad.device_id
        AND oa.`date` = ad.`date`
        AND oa.org_id = ad.org_id
    LEFT JOIN clouddb_organizations o ON oa.org_id = o.id
    JOIN definitions_harsh_accel_type_enums ha -- enums may be present in osdaccelerometer, but not in harsh_accel_type_enums
        ON oa.value.proto_value.accelerometer_event.harsh_accel_type = ha.enum
    WHERE isnotnull(oa.value.proto_value.accelerometer_event.harsh_accel_type) -- small number of events will invalid events
    ),

    firmware_events_cms AS (
    -- The inner join - oa.object_id = devices_cms.camera_id - will result in only those events where the
    -- object_id represents a camera_id.
    SELECT
        oa.date
        ,oa.time
        ,oa.org_id
        ,oa.object_id
        ,oa.value.proto_value.accelerometer_event.event_id
        ,dc.device_id
        ,dc.vg_product_name
        ,dc.camera_id
        ,dc.cm_product_name
        ,db_vg.latest_build_on_day AS vg_build
        ,db_cm.latest_build_on_day AS cm_build
        ,ad.total_distance AS distance_traveled
        ,CASE WHEN o.release_type_enum = 0 THEN "Phase 1"
        WHEN o.release_type_enum = 1 THEN "Phase 2"
        WHEN o.release_type_enum = 2 THEN "Early Adopter"
        END AS customer_phase
        ,CASE WHEN o.internal_type = 0 THEN "Customer Org"
        WHEN o.internal_type = 1 THEN "Internal Org"
        WHEN o.internal_type = 2 THEN "Lab Org"
        WHEN o.internal_type = 3 THEN "Developer Portal Test Org"
        WHEN o.internal_type = 4 THEN "Developer Portal Dev Org"
        END AS customer_type
        ,ha.event_type AS harsh_event_type
    FROM kinesisstats_history_osdaccelerometer oa -- use _history in case of backfill
    -- **Note the difference in join here as compared to firmware_events_vgs.**
    JOIN devices_cms dc ON oa.object_id = dc.camera_id
        AND oa.org_id = dc.org_id
    LEFT JOIN dataprep_device_builds db_vg ON dc.device_id = db_vg.device_id
        AND oa.`date` = db_vg.`date`
        AND oa.org_id = db_vg.org_id
    LEFT JOIN dataprep_device_builds db_cm ON dc.camera_id = db_cm.device_id
        AND oa.`date` = db_cm.`date`
        AND oa.org_id = db_cm.org_id
    LEFT JOIN dataprep_active_devices ad ON dc.device_id = ad.device_id
        AND oa.`date` = ad.`date`
        AND oa.org_id = ad.org_id
    LEFT JOIN clouddb_organizations o ON oa.org_id = o.id
    JOIN definitions_harsh_accel_type_enums ha -- enums may be present in osdaccelerometer, but not in harsh_accel_type_enums
        ON oa.value.proto_value.accelerometer_event.harsh_accel_type = ha.enum
    WHERE isnotnull(oa.value.proto_value.accelerometer_event.harsh_accel_type) -- small number of events will invalid events
    ),

    combined_events AS (
        SELECT * FROM firmware_events_vgs
        UNION
        SELECT * FROM firmware_events_cms
    ),

    firmware_staging AS (
        SELECT *,
            row_number() OVER (PARTITION BY `date`, org_id, object_id, `time`, event_id ORDER BY `time` DESC) AS rnk
        FROM combined_events
    )

    SELECT
        `date`,
        `time`,
        org_id,
        object_id,
        event_id,
        device_id,
        vg_product_name,
        camera_id,
        cm_product_name,
        vg_build,
        cm_build,
        distance_traveled,
        customer_phase,
        customer_type,
        harsh_event_type
    FROM firmware_staging
    WHERE rnk = 1

    --endsql"""
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.sql(query)
    return df


dqs["firmware_events"].append(
    PrimaryKeyDQCheck(
        name="firmware_events_primary_key_check",
        database="dataengineering",
        table="firmware_events",
        primary_keys=[
            "date",
            "time",
            "org_id",
            "object_id",
            "event_id",
        ],  # in future will try to reuse asset metadata
        blocking=False,
    )
)

dqs["firmware_events"].append(
    NonEmptyDQCheck(
        name="firmware_events_non_empty_check",
        database="dataengineering",
        table="firmware_events",
        blocking=False,
    )
)

dqs["firmware_events"].append(
    NonNullDQCheck(
        name="firmware_events_non_null_check",
        database="dataengineering",
        table="firmware_events",
        non_null_columns=[
            "date",
            "time",
            "org_id",
            "object_id",
        ],
        blocking=False,
    )
)


dq_assets = dqs.generate()
