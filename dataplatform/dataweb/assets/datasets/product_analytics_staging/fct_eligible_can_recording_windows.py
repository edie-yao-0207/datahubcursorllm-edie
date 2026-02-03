"""
Eligible CAN Recording Windows

This table identifies eligible CAN recording windows that meet quality and availability criteria
for CAN trace processing. Filters recording windows based on device compatibility (VG devices with
camera attachments), organizational requirements (SFDC account availability), and product constraints
(CM3/4 products). Includes trip association analysis using extended temporal windows to identify
recordings that occurred during active trips. Provides foundation data for downstream CAN trace
processing and analysis by pre-qualifying recording windows based on technical and business requirements.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataclasses import replace
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import (
    ProductAnalyticsStaging,
    DataModelCore,
    DataModelTelematics,
    CloudDb,
    Definitions,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .fct_can_recording_windows import (
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    CAN_0_NAME_COLUMN,
    CAN_1_NAME_COLUMN,
)

QUERY = """
WITH disallowed_sam_numbers AS (
  SELECT col1 AS sam_number
   FROM VALUES
    ('CN3MKNDWE2'),
    ('CH4ZVRNBRF'),
    ('CKH4UXEKTJ'),
    ('CRB4VEWFDT'),
    ('C5W2ZCYT2T'),
    ('C34NJKETE4'),
    ('CB3722ZUPZ'),
    ('CVZJ9972XK'),
    ('CYXENWAU2N'),
    ('CK4S3KZ6KX'),
    ('CYWK5RTMS3'),
    ('CNHBP37GRF'),
    ('CUW47E4SCW'),
    ('CHDVRJ2UWA'),
    ('CUVAN2WCC2'),
    ('CDZ4XBJP44'),
    ('CB36B9JYVW'),
    ('CX3J2U3WR4'),
    ('C2U6U24BYJ'),
    ('CT2DFF4F6X'),
    ('CJ58DTGPKY'),
    ('CJNB2VGHAF'),
    ('CYF96DBDPM'),
    ('C89KUDCYJV'),
    ('CRSMDSJW6U'),
    ('CRDR7ZNGA5'),
    ('C9J6GG99A6'),
    ('CNAKWSP53J'),
    ('C5RNY8V97F'),
    ('CZNZPK9E7R'),
    ('C4XNJ37GWH'),
    ('CEU63FDNAY'),
    ('CCEX6FDZ6F'),
    ('CS45KWNSS8'),
    ('CN6U9BK3BW'),
    ('CBHXW66GM7'),
    ('C3KX3SC387'),
    ('CE2CAWK5TZ'),
    ('CJG2VG4SV8'),
    ('CRFNJ4MR5U'),
    ('C9J6GG99A6'),
    ('CNAKWSP53J'),
    ('C9NJ74WEPU'),
    ('CMBXERK9K7'),
    ('CZS44R8WVH'),
    ('C2PMS2BEZE'),
    ('CCPJ37W4ZA'),
    ('C2DC7CEMVH'),
    ('C5UMVRSNHB'),
    ('CKNWU8B87C'),
    ('C3VGBRTYR2')
),
disallowed_orgs AS (
  SELECT col1 AS org_id
  FROM VALUES
    (562949953429992),
    (562949953429994),
    (562949953429995),
    (562949953429996),
    (562949953429997),
    (562949953429998),
    (562949953429999),
    (562949953430000),
    (562949953430001),
    (562949953430012),
    (562949953430002),
    (562949953430003),
    (562949953430004),
    (562949953430013),
    (6000290),
    (4000276)
),
traces AS (
    SELECT
        rw.date,
        rw.org_id,
        device_id,
        start_time,
        end_time,
        capture_duration,
        can_0_name,
        can_1_name

    FROM product_analytics_staging.fct_can_recording_windows rw
    JOIN datamodel_core.dim_devices AS device USING (date, org_id, device_id)
    JOIN definitions.products ON device.associated_devices.camera_product_id = definitions.products.product_id
    JOIN clouddb.org_sfdc_accounts sfdc USING (org_id)
    JOIN datamodel_core.dim_organizations orgs USING (date, org_id)
    LEFT JOIN disallowed_sam_numbers
      ON orgs.sam_number = disallowed_sam_numbers.sam_number
    LEFT JOIN disallowed_orgs
      ON rw.org_id = disallowed_orgs.org_id

    -- Only include traces from orgs that have an SFDC account (possible to pull historical data)
    -- Only include devices that have a CM attached. Trace windows can be created without a CM attached.
    -- We currently only support traces to be stored on CMs
    -- Exclude disallowed SAM numbers and org IDs
    WHERE rw.date BETWEEN "{date_start}" AND "{date_end}"
      AND device_type LIKE "VG%"
      AND associated_devices.camera_device_id IS NOT NULL
      AND definitions.products.name RLIKE "CM3[1-4]"
      AND sfdc.org_id IS NOT NULL
      AND disallowed_sam_numbers.sam_number IS NULL
      AND disallowed_orgs.org_id IS NULL
),

trips AS (
    SELECT
        date,
        org_id,
        device_id,
        start_time_ms,
        end_time_ms
    FROM datamodel_telematics.fct_trips
    WHERE date BETWEEN DATE_SUB("{date_start}", 1) AND "{date_end}"
),

traces_with_trip_status AS (
    SELECT
        trace.date,
        trace.org_id,
        trace.device_id,
        trace.start_time,
        trace.end_time,
        trace.capture_duration,
        trace.can_0_name,
        trace.can_1_name,
        MAX(trip.device_id IS NOT NULL) AS on_trip
    FROM traces AS trace
    LEFT JOIN trips AS trip ON (
        trace.date = trip.date
        AND trace.org_id = trip.org_id
        AND trace.device_id = trip.device_id
        AND trace.start_time >= trip.start_time_ms
        AND trace.end_time <= trip.end_time_ms
    )
    GROUP BY ALL
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    can_0_name,
    can_1_name,
    on_trip
FROM traces_with_trip_status
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    CAN_0_NAME_COLUMN,
    CAN_1_NAME_COLUMN,
    Column(
        name="on_trip",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether the CAN recording window occurred during an active trip using extended trip context for boundary-spanning detection."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table contains entries from fct_can_recording_windows that meet minimum eligibility requirements for retrieval. "
        "Applies filtering criteria including VG device compatibility with camera attachments, SFDC account availability for "
        "historical data access, and CM3/4 product constraints for trace storage compatibility. Trip status is included as a "
        "field since it is highly likely to be relevant to most queries retrieving traces, enabling efficient filtering on "
        "trip-associated recordings without requiring additional joins to trip data.",
        row_meaning="Each row represents a CAN recording window that has been qualified as eligible for trace retrieval based on technical and business requirements.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    single_run_backfill=True,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_RECORDING_WINDOWS),
        AnyUpstream(DataModelCore.DIM_DEVICES),
        AnyUpstream(DataModelCore.DIM_ORGANIZATIONS),
        AnyUpstream(Definitions.PRODUCTS),
        AnyUpstream(DataModelTelematics.FCT_TRIPS),
        AnyUpstream(CloudDb.ORG_SFDX_ACCOUNTS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ELIGIBLE_CAN_RECORDING_WINDOWS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_eligible_can_recording_windows(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
