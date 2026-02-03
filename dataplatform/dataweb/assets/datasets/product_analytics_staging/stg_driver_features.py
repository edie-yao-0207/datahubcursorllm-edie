from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "driver_id"]

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
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID,
        },
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_count_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_duration_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_distance_miles_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_duration_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "camera_trip_distance_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_duration_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dual_facing_camera_trip_distance_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_speed_kmph_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "over_speed_limit_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "light_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "moderate_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "heavy_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "severe_speeding_mins_1d",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_in_morning_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_ended_at_night_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "is_eld_relevant_device",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_below_freezing_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_count_over_35_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "max_trip_end_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crashes_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "speeding_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "following_distance_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "mobile_usage_violations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_braking_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_turns_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "no_seatbelt_violations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "distracted_driving_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "rolling_stop_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "harsh_accelerations_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "drowsiness_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "lane_departure_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "total_events_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "coached_events_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "virtual_coaching_1d",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "total_driving_environment_locations",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_rural_driving_environment_locations",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_urban_driving_environment_locations",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_mixed_driving_environment_locations",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_rural_driving_environment_locations",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_urban_driving_environment_locations",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_mixed_driving_environment_locations",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_total_locations",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_living_street",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_motorway",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_primary",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_residential",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_secondary",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_service",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_tertiary",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_trunk",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_unclassified",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "count_roadway_null",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_living_street",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_motorway",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_primary",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_residential",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_secondary",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_service",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_tertiary",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_trunk",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_unclassified",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "proportion_roadway_null",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "hos_violation_count_1d",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_vehicle_model",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_vehicle_make",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_max_weight_lbs",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "primary_gross_vehicle_weight_rating",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofweek_sin",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofweek_cos",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofyear_sin",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "dayofyear_cos",
        "type": "double",
        "nullable": False,
        "metadata": {},
    }
]

QUERY = """
WITH combined_licensed_orgs AS (
  SELECT
      o.date,
      o.org_id,
      o.account_cs_tier,
      o.account_size_segment,
      o.account_arr_segment,
      o.is_paid_safety_customer,
      o.is_paid_telematics_customer
  FROM
      datamodel_core.dim_organizations o
  WHERE
      o.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
      AND o.is_paid_safety_customer = TRUE
),
-- Filter devices based on VG  product IDs
filtered_devices AS (
  SELECT
    date,
    org_id,
    device_id,
    associated_devices.camera_product_id AS camera_product_id,
    associated_vehicle_model as vehicle_model,
    associated_vehicle_make as vehicle_make,
    case when associated_vehicle_max_weight_lbs < 1 then null else associated_vehicle_max_weight_lbs end as max_weight_lbs,
    associated_vehicle_gross_vehicle_weight_rating as gross_vehicle_weight_rating
  FROM
    datamodel_core.dim_devices
  WHERE
    product_id IN (7, 17, 24, 35, 53, 89, 90, 178) -- VG product IDs
    AND date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
),
-- Get trips for filtered devices and filtered orgs
eligible_trips_pre AS (
    SELECT DISTINCT
        trips.date,
        trips.org_id,
        trips.device_id,
        trips.driver_id,
        trips.start_time_ms,
        trips.end_time_ms,
        trips.duration_mins,
        trips.distance_miles,
        trips.max_speed_kmph,
        trips.over_speed_limit_mins,
        trips.light_speeding_mins,
        trips.moderate_speeding_mins,
        trips.heavy_speeding_mins,
        trips.severe_speeding_mins,
        trips.end_state,
        trips.end_time_utc,
        CASE WHEN p.name IN ('CM32', 'CM34') THEN 1 ELSE 0 END AS has_dual_facing_camera,
        CASE WHEN p.name IS NOT NULL THEN 1 ELSE 0 END AS has_camera,
        lo.account_cs_tier,
        lo.account_size_segment,
        lo.account_arr_segment,
        fd.vehicle_model,
        fd.vehicle_make,
        fd.max_weight_lbs,
        fd.gross_vehicle_weight_rating
    FROM
        datamodel_telematics.fct_trips trips
    INNER JOIN filtered_devices fd
        ON fd.device_id = trips.device_id
        AND fd.org_id = trips.org_id
        AND fd.date = trips.date
    INNER JOIN combined_licensed_orgs lo
        ON lo.org_id = trips.org_id
        AND lo.date = trips.date
    LEFT JOIN definitions.products p
        ON fd.camera_product_id = p.product_id
    WHERE
        trips.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
        AND trips.driver_id IS NOT NULL
        AND trips.driver_id != 0
        AND trips.trip_type = 'location_based'
),
eligible_trips_timezones AS (
  SELECT
    et.*,
    CASE
        WHEN et.end_state IN ('ME', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE',
                                'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'VT',
                                'OH', 'MI', 'IN', 'KY', 'ON', 'QC', 'ROO') THEN 'Eastern'
        WHEN et.end_state IN ('WI', 'IL', 'MN', 'IA', 'MO', 'AR', 'ND', 'SD', 'NE', 'KS',
                                'OK', 'TX', 'TN', 'AL', 'MS', 'LA',
                                'MB', 'SK',
                                'MEX', 'NLE', 'TAM', 'JAL', 'GUA', 'CHH', 'DIF', 'VER',
                                'QUE', 'SLP', 'COA', 'HID', 'PUE', 'MIC', 'COL', 'YUC',
                                'AGU', 'TLA', 'TAB', 'ZAC', 'CHP', 'DUR', 'MOR', 'OAX',
                                'CAM', 'GRO') THEN 'Central'
        WHEN et.end_state IN ('MT', 'WY', 'CO', 'NM', 'ID', 'UT', 'AZ', 'AB', 'SON', 'SIN', 'NAY', 'BCS', 'NT') THEN 'Mountain'
        WHEN et.end_state IN ('CA', 'OR', 'WA', 'NV', 'AK', 'BC', 'BCN', 'YT', 'NU') THEN 'Pacific'
        WHEN et.end_state IN ('NB', 'NS', 'NL', 'PE', 'PR') THEN 'Atlantic'
        WHEN et.end_state IN ('GU') THEN 'Chamorro'
        WHEN et.end_state IN ('HI') THEN 'Hawaii' END AS timezone,
    et.end_time_utc
  FROM eligible_trips_pre et),
eligible_trips AS (
  SELECT
    ett.*,
    CASE
      WHEN (
        (ett.timezone = 'Chamorro' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (10 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (10 * 3600))) < 5)) OR
        (ett.timezone = 'Atlantic' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-4 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-4 * 3600))) < 5)) OR
        (ett.timezone = 'Eastern' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-5 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-5 * 3600))) < 5)) OR
        (ett.timezone = 'Central' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-6 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-6 * 3600))) < 5)) OR
        (ett.timezone = 'Mountain' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-7 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-7 * 3600))) < 5)) OR
        (ett.timezone = 'Pacific' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-8 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-8 * 3600))) < 5)) OR
        (ett.timezone = 'Hawaii' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-10 * 3600))) >= 21 OR HOUR(from_unixtime(ett.end_time_ms / 1000 + (-10 * 3600))) < 5))
      ) THEN 1 ELSE 0
    END AS ended_at_night,
    CASE
      WHEN (
        (ett.timezone = 'Chamorro' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (10 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (10 * 3600))) < 9)) OR
        (ett.timezone = 'Atlantic' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-4 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-4 * 3600))) < 9)) OR
        (ett.timezone = 'Eastern' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-5 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-5 * 3600))) < 9)) OR
        (ett.timezone = 'Central' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-6 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-6 * 3600))) < 9)) OR
        (ett.timezone = 'Mountain' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-7 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-7 * 3600))) < 9)) OR
        (ett.timezone = 'Pacific' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-8 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-8 * 3600))) < 9)) OR
        (ett.timezone = 'Hawaii' AND (HOUR(from_unixtime(ett.end_time_ms / 1000 + (-10 * 3600))) >= 4 AND HOUR(from_unixtime(ett.end_time_ms / 1000 + (-10 * 3600))) < 9))
      ) THEN 1 ELSE 0
    END AS ended_in_morning
  FROM eligible_trips_timezones ett),
eld_relevance AS (
  SELECT DISTINCT
    deld.org_id,
    deld.date
  FROM
    datamodel_telematics.dim_eld_relevant_devices deld
  INNER JOIN combined_licensed_orgs clo ON deld.org_id = clo.org_id
  WHERE
    deld.is_eld_relevant = TRUE
    AND deld.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'),
hos_violations_pre AS (
  SELECT DISTINCT
    DATE(start_at) AS start_date,
    DATE(COALESCE(end_at, date)) AS end_date,
    driver_id,
    org_id,
    type
  FROM eldhosdb_shards.driver_hos_violations
  WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    AND start_at <= '{PARTITION_END}'
    AND (deleted_at = TIMESTAMP '+0101' OR deleted_at IS NULL)
    AND type IN (
      -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
      1, -- SHIFT_HOURS
      2, -- SHIFT_DRIVING_HOURS
      100, -- RESTBREAK_MISSED
      101, -- CALIFORNIA_MEALBREAK_MISSED
      200, -- CYCLE_HOURS_ON
      201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
      1000, -- SHIFT_ON_DUTY_HOURS
      1001, -- DAILY_ON_DUTY_HOURS
      1002, -- DAILY_DRIVING_HOURS
      1009 -- MANDATORY_24_HOURS_OFF_DUTY
    )
),
hos_violations_exploded AS (
  SELECT
    start_date,
    end_date,
    violation_date,
    driver_id,
    org_id,
    type
  FROM hos_violations_pre
  LATERAL VIEW explode(sequence(start_date, end_date, interval 1 day)) AS violation_date
),
hos_violations AS (
  SELECT
    hve.violation_date AS date,
    hve.driver_id,
    hve.org_id,
    1 AS hos_violation_count_1d
  FROM hos_violations_exploded hve
  INNER JOIN eld_relevance er
    ON hve.org_id = er.org_id
    AND hve.violation_date = er.date
  WHERE hve.violation_date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
  GROUP BY ALL
),
engine_gauge AS (
SELECT *
        FROM kinesisstats_history.osdenginegauge
        WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
        AND value.is_databreak = false
        AND value.is_end = false),
trip_air_temperature AS (
SELECT
    trips.org_id,
    trips.device_id,
    trips.date,
    trips.start_time_ms,
    MIN(engine_gauge.value.proto_value.engine_gauge_event.air_temp_milli_c / 1000) AS air_temp_c_start
  FROM eligible_trips trips
  INNER JOIN  engine_gauge
    ON engine_gauge.org_id = trips.org_id
    AND engine_gauge.object_id = trips.device_id
    AND engine_gauge.time BETWEEN (trips.start_time_ms - 60000) AND (trips.start_time_ms + 60000)
    AND engine_gauge.date = trips.date
  GROUP BY ALL),
trip_statistics AS (
SELECT
  trips.date,
  trips.org_id,
  trips.driver_id,
  FIRST(trips.account_cs_tier) AS account_cs_tier,
  FIRST(trips.account_size_segment) AS account_size_segment,
  FIRST(trips.account_arr_segment) AS account_arr_segment,
  COUNT(*) AS trip_count_1d,
  SUM(trips.has_camera) AS camera_trip_count_1d,
  SUM(trips.has_dual_facing_camera) AS dual_facing_camera_trip_count_1d,
  SUM(trips.duration_mins) AS trip_duration_mins_1d,
  SUM(trips.distance_miles) AS trip_distance_miles_1d,
  SUM(CASE WHEN trips.has_camera = TRUE THEN trips.duration_mins ELSE 0 END) AS camera_trip_duration_mins_1d,
  SUM(CASE WHEN trips.has_camera = TRUE THEN trips.distance_miles ELSE 0 END) AS camera_trip_distance_1d,
  SUM(CASE WHEN trips.has_dual_facing_camera = TRUE THEN trips.duration_mins ELSE 0 END) AS dual_facing_camera_trip_duration_mins_1d,
  SUM(CASE WHEN trips.has_dual_facing_camera = TRUE THEN trips.distance_miles ELSE 0 END) AS dual_facing_camera_trip_distance_1d,
  MAX(CASE WHEN trips.max_speed_kmph < 200.0 THEN trips.max_speed_kmph ELSE NULL END) AS max_speed_kmph_1d,
  SUM(trips.light_speeding_mins + trips.moderate_speeding_mins + trips.heavy_speeding_mins + trips.severe_speeding_mins) AS over_speed_limit_mins_1d,
  SUM(trips.light_speeding_mins) AS light_speeding_mins_1d,
  SUM(trips.moderate_speeding_mins) AS moderate_speeding_mins_1d,
  SUM(trips.heavy_speeding_mins) AS heavy_speeding_mins_1d,
  SUM(trips.severe_speeding_mins) AS severe_speeding_mins_1d,
  SUM(trips.ended_in_morning) AS trip_ended_in_morning_1d,
  SUM(trips.ended_at_night) AS trip_ended_at_night_1d,
  CASE WHEN FIRST(eld_relevance.org_id) IS NOT NULL THEN 1 ELSE 0 END AS is_eld_relevant_device,
  SUM(CASE WHEN trip_air_temperature.air_temp_c_start < 0 THEN 1 ELSE 0 END) AS trip_count_below_freezing_1d,
  SUM(CASE WHEN trip_air_temperature.air_temp_c_start > 35 THEN 1 ELSE 0 END) AS trip_count_over_35_1d,
  MAX(trips.end_time_ms) AS max_trip_end_time_ms
FROM eligible_trips trips
LEFT JOIN eld_relevance
  ON eld_relevance.org_id = trips.org_id
  AND eld_relevance.date = trips.date
LEFT JOIN trip_air_temperature
  ON trip_air_temperature.org_id = trips.org_id
  AND trip_air_temperature.device_id = trips.device_id
  AND trip_air_temperature.start_time_ms = trips.start_time_ms
  AND trip_air_temperature.date = trips.date
GROUP BY 1,2,3),
vehicle_statistics_pre AS (
SELECT
  date,
  org_id,
  driver_id,
  vehicle_model,
  vehicle_make,
  max_weight_lbs,
  gross_vehicle_weight_rating,
  SUM(duration_mins) AS duration_mins_1d
FROM eligible_trips
GROUP BY ALL),
vehicle_statistics AS (
SELECT date,
       org_id,
       driver_id,
       MAX_BY(vehicle_model, duration_mins_1d) AS primary_vehicle_model,
       MAX_BY(vehicle_make, duration_mins_1d) AS primary_vehicle_make,
       CASE WHEN MAX_BY(max_weight_lbs, duration_mins_1d) > 0
            THEN MAX_BY(max_weight_lbs, duration_mins_1d)
            ELSE -1 END AS primary_max_weight_lbs,
       MAX_BY(gross_vehicle_weight_rating, duration_mins_1d) AS primary_gross_vehicle_weight_rating
FROM vehicle_statistics_pre
GROUP BY ALL),
safety_statistics AS (
SELECT
    e.date,
    e.org_id,
    e.object_id as driver_id,
    SUM(e.crash_count) AS crashes_1d,
    SUM(e.speeding_count + e.severe_speeding_count + e.heavy_speeding_count) AS speeding_1d,
    SUM(e.following_distance_count) AS following_distance_1d,
    SUM(e.mobile_usage_count) + SUM(e.edge_mobile_usage_count) AS mobile_usage_violations_1d,
    SUM(e.braking_count)AS harsh_braking_1d,
    SUM(e.harsh_turn_count) AS harsh_turns_1d,
    SUM( e.no_seatbelt_count) AS no_seatbelt_violations_1d,
    SUM(e.generic_distraction_count + e.edge_distracted_driving_count) AS distracted_driving_1d,
    SUM(e.rolling_stop_count) AS rolling_stop_1d,
    SUM(e.acceleration_count) AS harsh_accelerations_1d,
    SUM(e.drowsy_count) AS drowsiness_1d,
    SUM(e.lane_departure_count ) AS lane_departure_1d,
    SUM( e.total_event_count + e.severe_speeding_count + e.heavy_speeding_count) AS total_events_1d
  FROM
    safety_report.score_event_report_v4 e
  WHERE e.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
  AND e.object_id != 0
  AND e.object_id IS NOT NULL
  AND e.object_type = 5 -- driver
  GROUP BY 1,2,3),
  trip_roadway AS (
  SELECT date,
       org_id,
       driver_id,
       SUM(count_total_locations) AS count_total_locations,
       SUM(count_living_street) AS count_roadway_living_street,
       SUM(count_motorway) AS count_roadway_motorway,
       SUM(count_primary) AS count_roadway_primary,
       SUM(count_residential) AS count_roadway_residential,
       SUM(count_secondary) AS count_roadway_secondary,
       SUM(count_service) AS count_roadway_service,
       SUM(count_tertiary) AS count_roadway_tertiary,
       SUM(count_trunk) AS count_roadway_trunk,
       SUM(count_unclassified) AS count_roadway_unclassified,
       SUM(count_null_osm_tag) AS count_roadway_null,
       SUM(count_living_street) / SUM(count_total_locations) AS proportion_roadway_living_street,
       SUM(count_motorway) / SUM(count_total_locations) AS proportion_roadway_motorway,
       SUM(count_primary) / SUM(count_total_locations) AS proportion_roadway_primary,
       SUM(count_residential) / SUM(count_total_locations) AS proportion_roadway_residential,
       SUM(count_secondary) / SUM(count_total_locations) AS proportion_roadway_secondary,
       SUM(count_service) / SUM(count_total_locations) AS proportion_roadway_service,
       SUM(count_tertiary) / SUM(count_total_locations) AS proportion_roadway_tertiary,
       SUM(count_trunk) / SUM(count_total_locations) AS proportion_roadway_trunk,
       SUM(count_unclassified) / SUM(count_total_locations) AS proportion_roadway_unclassified,
       SUM(count_null_osm_tag) / SUM(count_total_locations) AS proportion_roadway_null
FROM product_analytics_staging.agg_trip_roadway
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY 1,2,3),
driving_environment AS (
SELECT date,
       org_id,
       driver_id,
       SUM(total_locations) AS total_driving_environment_locations,
       SUM(count_rural_locations) AS count_rural_driving_environment_locations,
       SUM(count_urban_locations) AS count_urban_driving_environment_locations,
       SUM(count_mixed_locations) AS count_mixed_driving_environment_locations,
       SUM(count_rural_locations) / SUM(total_locations) AS proportion_rural_driving_environment_locations,
       SUM(count_urban_locations) / SUM(total_locations) AS proportion_urban_driving_environment_locations,
       SUM(count_mixed_locations) / SUM(total_locations) AS proportion_mixed_driving_environment_locations
FROM product_analytics_staging.agg_trip_driving_environment
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
GROUP BY 1,2,3 ),
coached_events AS (
    SELECT
        date,
        org_id,
        driver_id,
        COUNT(DISTINCT event_id) AS coached_events_1d
    FROM
        datamodel_safety.fct_safety_events
    LATERAL VIEW EXPLODE(coaching_state_details_array) AS coaching_state
    WHERE
        coaching_state.coaching_state_name = 'Coached'
        AND driver_id IS NOT NULL AND driver_id != 0
        AND date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    GROUP BY 1,2,3
),
virtual_coaching AS (
    SELECT
        date,
        org_id,
        driver_id,
        COUNT(DISTINCT uuid) AS virtual_coaching_1d
    FROM coachingdb_shards.coaching_sessions_behavior
    WHERE
        date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
        AND coaching_status = 100
        AND driver_coached = 1
        AND driver_id IS NOT NULL AND driver_id != 0
    GROUP BY 1,2,3
)

  SELECT trips.*,
         safety.* EXCEPT (date, org_id, driver_id),
         driving_environment.* EXCEPT (date, org_id, driver_id),
         trip_roadway.* EXCEPT (date, org_id, driver_id),
         COALESCE(hos_violations.hos_violation_count_1d, 0) AS hos_violation_count_1d,
         COALESCE(coached_events.coached_events_1d, 0) AS coached_events_1d,
         COALESCE(virtual_coaching.virtual_coaching_1d, 0) AS virtual_coaching_1d,
         vehicle_statistics.* EXCEPT (date, org_id, driver_id),
         sin((2 * pi() * (dayofweek(trips.date)-1)) / 7) AS dayofweek_sin,
         cos((2 * pi() * (dayofweek(trips.date)-1)) / 7) AS dayofweek_cos,
         sin((2 * pi() * (dayofyear(trips.date)-1)) / 365.25) AS dayofyear_sin,
         cos((2 * pi() * (dayofyear(trips.date)-1)) / 365.25) AS dayofyear_cos
  FROM trip_statistics trips
  LEFT JOIN safety_statistics safety
    ON safety.org_id = trips.org_id
    AND safety.driver_id = trips.driver_id
    AND safety.date = trips.date
  LEFT JOIN driving_environment
   ON driving_environment.org_id = trips.org_id
   AND driving_environment.driver_id = trips.driver_id
   AND driving_environment.date = trips.date
  LEFT JOIN trip_roadway
   ON trip_roadway.org_id = trips.org_id
   AND trip_roadway.driver_id = trips.driver_id
   AND trip_roadway.date = trips.date
  LEFT JOIN hos_violations
   ON hos_violations.org_id = trips.org_id
   AND hos_violations.driver_id = trips.driver_id
   AND hos_violations.date = trips.date
   LEFT JOIN coached_events
   ON coached_events.org_id = trips.org_id
   AND coached_events.driver_id = trips.driver_id
   AND coached_events.date = trips.date
   LEFT JOIN virtual_coaching
   ON virtual_coaching.org_id = trips.org_id
   AND virtual_coaching.driver_id = trips.driver_id
   AND virtual_coaching.date = trips.date
   LEFT JOIN vehicle_statistics
   ON vehicle_statistics.org_id = trips.org_id
   AND vehicle_statistics.driver_id = trips.driver_id
   AND vehicle_statistics.date = trips.date
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Calculation of driver features sourced from trip, safety, and location data for a date.""",
        row_meaning="""Each row represents trip, safety, and organizational metadata for a driver for a date.""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2023-09-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_driver_features"),
        PrimaryKeyDQCheck(name="dq_pk_stg_driver_features", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_stg_driver_features",
            non_null_columns=PRIMARY_KEYS,
        ),
    ],
    backfill_batch_size=3,
    upstreams=["datamodel_telematics.fct_trips",
               "datamodel_core.dim_organizations",
               "safety_report.score_event_report_v4",
               "kinesisstats_history.osdenginegauge",
               "datamodel_telematics.dim_eld_relevant_devices",
               "datamodel_core.dim_devices",
               "product_analytics_staging.agg_trip_driving_environment",
               "product_analytics_staging.agg_trip_roadway",
               "eldhosdb_shards.driver_hos_violations",
               "datamodel_safety.fct_safety_events",
               "coachingdb_shards.coaching_sessions_behavior"],
)
def stg_driver_features(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_driver_features")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
