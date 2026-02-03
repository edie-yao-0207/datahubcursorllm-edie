from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description

PRIMARY_KEYS = ["crash_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "report_state",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "timezone",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "report_number",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "report_date",
        "type": "date",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "report_timestamp",
        "type": "timestamp",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "report_seq_no",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dot_number",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "ci_status_code",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "final_status_date",
        "type": "date",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "location",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "city_code",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "city",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "state",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "county_code",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "truck_bus_ind",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trafficway_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "access_control_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "road_surface_condition_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "cargo_body_type_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "gvwr_rating_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicle_identification_number",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicle_license_number",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicle_lic_state",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicle_hazmat_placard",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "weather_condition_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicle_configuration_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_condition_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "hazmat_released",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "agency",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "vehicles_in_accident",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "fatalities",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "injuries",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "tow_away",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "federal_recordable",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "state_recordable",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_id",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_name",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_street",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_city",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_city_code",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_state",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_zip_code",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_carrier_interstate",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_event_seq_id_desc",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
]

query = """
with base as
(
  select *,
  concat(report_date, ' ', report_time) as report_date_time,
  CASE
    WHEN report_state IN ('CT','DE','DC','FL','GA','IN','KY','ME','MD','MA',
                          'MI','NH','NJ','NY','NC','OH','PA','RI','SC','TN',
                          'VA','VT','WV') THEN 'America/New_York'
    WHEN report_state IN ('AL','AR','IA','IL','KS','LA','MN','MO','MS','ND',
                          'NE','OK','SD','TX','WI') THEN 'America/Chicago'
    WHEN report_state IN ('CO','ID','MT','NM','UT','WY') THEN 'America/Denver'
    WHEN report_state = 'AZ' THEN 'America/Phoenix'
    WHEN report_state IN ('CA','NV','OR','WA') THEN 'America/Los_Angeles'
    WHEN report_state = 'AK' THEN 'America/Anchorage'
    WHEN report_state = 'HI'THEN 'Pacific/Honolulu'
    WHEN report_state = 'PR' THEN 'America/Puerto_Rico'
    ELSE NULL
END AS timezone
from read_files(
        's3://samsara-databricks-workspace/dataengineering/csvs/Crash_File_20251121.csv',
        format => 'csv',
        header => true)
where report_state in ('CT','DE','DC','FL','GA','IN','KY','ME','MD','MA',
                        'MI','NH','NJ','NY','NC','OH','PA','RI','SC','TN',
                        'VA','VT','WV', 'AL','AR','IA','IL','KS','LA','MN',
                        'MO','MS','ND','NE','OK','SD','TX','WI','CO','ID',
                        'MT','NM','UT','WY', 'AZ','CA','NV','OR','WA','AK',
                        'HI', 'PR')
      and len(vehicle_identification_number) = 17
      and vehicle_identification_number NOT IN
        ('99999999999999999',
        '88888888888888888',
        '00000000000000000',
        'UNKNOWN9999999999',
        'NOVIN999999999999',
        '11111111111111111')
)

select
string(current_date()) as date,
to_utc_timestamp(to_timestamp(change_date, 'yyyyMMdd HHmm'),'America/New_York') as updated_at,
try_cast(crash_id as bigint) AS crash_id,
report_state,
timezone,
report_number,
date(to_utc_timestamp(to_timestamp(report_date, 'yyyyMMdd'), timezone)) as report_date,
to_utc_timestamp(to_timestamp(report_date_time, 'yyyyMMdd HHmm'), timezone) as report_timestamp,
try_cast(report_seq_no as bigint) as report_seq_no,
try_cast(dot_number as bigint) as dot_number,
ci_status_code,
date(to_utc_timestamp(to_timestamp(final_status_date, 'yyyyMMdd'), timezone)) as final_status_date,
location,
city_code,
city,
state,
county_code,
truck_bus_ind,
try_cast(trafficway_id as bigint) as trafficway_id,
try_cast(access_control_id as bigint) as access_control_id,
try_cast(road_surface_condition_id as bigint) as road_surface_condition_id,
try_cast(cargo_body_type_id as bigint) as cargo_body_type_id,
try_cast(gvw_rating_id as bigint) as gvwr_rating_id,
vehicle_identification_number,
vehicle_license_number,
vehicle_lic_state,
vehicle_hazmat_placard,
try_cast(weather_condition_id as bigint) as weather_condition_id,
try_cast(vehicle_configuration_id as bigint) as vehicle_configuration_id,
try_cast(light_condition_id as bigint) as light_condition_id,
hazmat_released,
agency,
try_cast(vehicles_in_accident as bigint) as vehicles_in_accident,
try_cast(fatalities as bigint) as fatalities,
try_cast(injuries as bigint) as injuries,
CASE WHEN tow_away = 'Y' THEN 1 ELSE 0 END AS tow_away,
TRY_CAST(federal_recordable as bigint) as federal_recordable,
TRY_CAST(state_recordable as bigint) as state_recordable,
TRY_CAST(crash_carrier_id as bigint) as crash_carrier_id,
crash_carrier_name,
crash_carrier_street,
crash_carrier_city,
crash_carrier_city_code,
crash_carrier_state,
crash_carrier_zip_code,
crash_carrier_interstate,
crash_event_seq_id_desc
from base
where try_cast(crash_id as bigint) > 0
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Import of FMCSA crash reports.""",
        row_meaning="""Each row represents a crash report, not limited to Samsara vehicles""",
        related_table_info={},
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=None,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_crash_reports"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_crash_reports",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_crash_reports",
            non_null_columns=[
                "vehicle_identification_number",
                "report_date",
                "crash_id",
            ],
            block_before_write=False,
        ),
    ],
)
def stg_crash_reports(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_crash_reports")
    context.log.info(f"{query}")
    return query
