import re
from dataclasses import dataclass, field
from typing import Any, Callable, List, Tuple

from dataweb.userpkgs.interval_helpers import create_intervals_with_state_value
from dataweb.userpkgs.schema import Column as SchemaColumn
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, lit


@dataclass
class ValueColumnMapping:
    value_column: str
    output_column: SchemaColumn

@dataclass
class ObjectStatSource:
    """
    Class to model the source table for diagnostic value(s). An instance of this class
    contains the metadata needed to query and transform the source table so it can be joined
    with others.
    """

    table_name: str
    value_columns: List[ValueColumnMapping]
    object_id_column: str = field(default="object_id")
    calculate_intervals: Callable[[DataFrame, str, int, int, Any], DataFrame] = field(
        default=create_intervals_with_state_value
    )
    time_interval_bucket_ms: int = field(default=5000)

    def _value_column_mapping(self, prefix: str = "value") -> List[Tuple[str, str]]:
        return [(column.value_column, column.output_column.name) for column in self.value_columns]


    @property
    def stat_name(self) -> str:
        return self.table_name.split(".")[-1]

    @property
    def value_column(self) -> Column:
        return col("value").alias(self.stat_name)

    def get_aliased_value_columns(self, prefix: str = "value") -> List[Column]:
        return [
            col(column).alias(alias)
            for column, alias in self._value_column_mapping(prefix)
        ]

    @property
    def final_table_value_columns(self) -> List[Column]:
        return [col(alias) for _, alias in self._value_column_mapping()]



def calculate_static_intervals(
    df: DataFrame,
    state_column: str,
    start_time: int,
    end_time: int,
    interval_length_seconds=5,
    spark = None,
) -> DataFrame:
    """
    Returns a dataframe with the `time` column renamed to `start_ms`, and an `end_ms` column
    which is start_ms plus a fixed interval.
    """

    return (
        df.withColumn("state_value", col(state_column))
        .select("org_id", "device_id", "date", "time", "state_value")
        .withColumnRenamed("time", "start_ms")
        .withColumn("end_ms", col("start_ms") + lit(interval_length_seconds) * lit(1000))
    )


stats = [
    # Original features explored for Predictive Maintenance
    ObjectStatSource(
        table_name="kinesisstats.location",
        object_id_column="device_id",
        calculate_intervals=calculate_static_intervals,
        value_columns=[
            ValueColumnMapping(value_column="value.latitude", output_column=SchemaColumn(name="latitude", type="double", nullable=False, comment="Latitude in degrees.")),
            ValueColumnMapping(value_column="value.longitude", output_column=SchemaColumn(name="longitude", type="double", nullable=False, comment="Longitude in degrees.")),
            ValueColumnMapping(value_column="value.gps_speed_meters_per_second", output_column=SchemaColumn(name="gps_speed_meters_per_second", type="double", nullable=True, comment="gps_speed_meters_per_second")),
            ValueColumnMapping(value_column="value.heading_degrees", output_column=SchemaColumn(name="heading_degrees", type="double", nullable=True, comment="heading_degrees")),
            ValueColumnMapping(value_column="value.altitude_meters", output_column=SchemaColumn(name="altitude_meters", type="double", nullable=True, comment="altitude_meters")),
            ValueColumnMapping(value_column="value.speed_limit_meters_per_second", output_column=SchemaColumn(name="speed_limit_meters_per_second", type="double", nullable=True, comment="speed_limit_meters_per_second")),
        ],
        time_interval_bucket_ms=5000,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdenginemilliknots",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdenginemilliknots", type="long", nullable=True, comment="osdenginemilliknots")),
        ],
        time_interval_bucket_ms=30105,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdodometer",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdodometer", type="long", nullable=True, comment="osdodometer")),
        ],
        time_interval_bucket_ms=31194,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdenginestate",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdenginestate", type="long", nullable=True, comment="osdenginestate")),
        ],
        time_interval_bucket_ms=3690538,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdengineseconds",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdengineseconds", type="long", nullable=True, comment="osdengineseconds")),
        ],
        time_interval_bucket_ms=194892,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdderivedfuelconsumed",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdderivedfuelconsumed", type="long", nullable=True, comment="osdderivedfuelconsumed")),
        ],
        time_interval_bucket_ms=698735,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osddeltafuelconsumed",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osddeltafuelconsumed", type="long", nullable=True, comment="osddeltafuelconsumed")),
        ],
        time_interval_bucket_ms=698740,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdcablevoltage",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdcablevoltage", type="long", nullable=True, comment="osdcablevoltage")),
        ],
        time_interval_bucket_ms=1200001,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdenginefault",
        value_columns=[
            ValueColumnMapping(value_column="value.proto_value.vehicle_fault_event.j1939_faults.mil_status", output_column=SchemaColumn(name="vehicle_fault_event_j1939_faults_mil_status", type="integer", nullable=True, comment="vehicle_fault_event_j1939_faults_mil_status")),
            ValueColumnMapping(value_column="value.proto_value.vehicle_fault_event.j1939_faults.red_lamp_status", output_column=SchemaColumn(name="vehicle_fault_event_j1939_faults_red_lamp_status", type="integer", nullable=True, comment="vehicle_fault_event_j1939_faults_red_lamp_status")),
        ],
        time_interval_bucket_ms=24302663,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdj1939dpfregenneededstatus",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdj1939dpfregenneededstatus", type="long", nullable=True, comment="osdj1939dpfregenneededstatus")),
        ],
        time_interval_bucket_ms=301051,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdj1939dpfregenforcedstatus",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdj1939dpfregenforcedstatus", type="long", nullable=True, comment="osdj1939dpfregenforcedstatus")),
        ],
        time_interval_bucket_ms=301051,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdj1939dpfregenpassivestatus",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdj1939dpfregenpassivestatus", type="long", nullable=True, comment="osdj1939dpfregenpassivestatus")),
        ],
        time_interval_bucket_ms=301051,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdj1939dpfregenactivestatus",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdj1939dpfregenactivestatus", type="long", nullable=True, comment="osdj1939dpfregenactivestatus")),
        ],
        time_interval_bucket_ms=301051,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdat1dpfsootloadpercent",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdat1dpfsootloadpercent", type="long", nullable=True, comment="osdat1dpfsootloadpercent")),
        ],
        time_interval_bucket_ms=306928,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdat1dpfashloadpercent",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdat1dpfashloadpercent", type="long", nullable=True, comment="osdat1dpfashloadpercent")),
        ],
        time_interval_bucket_ms=301092,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osddeftanklevelmillipercent",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osddeftanklevelmillipercent", type="long", nullable=True, comment="osddeftanklevelmillipercent")),
        ],
        time_interval_bucket_ms=652002,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdvehiclecurrentgear",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdvehiclecurrentgear", type="long", nullable=True, comment="osdvehiclecurrentgear")),
        ],
        time_interval_bucket_ms=99608,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdtireconditiondata",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdtireconditiondata", type="long", nullable=True, comment="osdtireconditiondata")),
        ],
        time_interval_bucket_ms=600000,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdseatbeltdriver",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdseatbeltdriver", type="long", nullable=True, comment="osdseatbeltdriver")),
        ],
        time_interval_bucket_ms=3863995,
    ),
    # Various enginegauge features may be useful for DPF, engine oil pressure, and engine overheating below
    ObjectStatSource(
        table_name="kinesisstats.osdenginegauge",
        value_columns=[
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.air_temp_milli_c", output_column=SchemaColumn(name="air_temp_milli_c", type="long", nullable=True, comment="air_temp_milli_c")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.barometer_pa", output_column=SchemaColumn(name="barometer_pa", type="long", nullable=True, comment="barometer_pa")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.battery_milli_v", output_column=SchemaColumn(name="battery_milli_v", type="long", nullable=True, comment="battery_milli_v")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.coolant_temp_milli_c", output_column=SchemaColumn(name="coolant_temp_milli_c", type="long", nullable=True, comment="coolant_temp_milli_c")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.engine_hours_secs", output_column=SchemaColumn(name="engine_hours_secs", type="long", nullable=True, comment="engine_hours_secs")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.engine_load_percent", output_column=SchemaColumn(name="engine_load_percent", type="long", nullable=True, comment="engine_load_percent")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.engine_on_secs", output_column=SchemaColumn(name="engine_on_secs", type="long", nullable=True, comment="engine_on_secs")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.engine_rpm", output_column=SchemaColumn(name="engine_rpm", type="long", nullable=True, comment="engine_rpm")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.fuel_economy_meters_per_k_g", output_column=SchemaColumn(name="fuel_economy_meters_per_k_g", type="long", nullable=True, comment="fuel_economy_meters_per_k_g")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.fuel_level_percent", output_column=SchemaColumn(name="fuel_level_percent", type="long", nullable=True, comment="fuel_level_percent")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.fuel_temp_milli_c", output_column=SchemaColumn(name="fuel_temp_milli_c", type="long", nullable=True, comment="fuel_temp_milli_c")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.mnfld_temp_milli_c", output_column=SchemaColumn(name="mnfld_temp_milli_c", type="long", nullable=True, comment="mnfld_temp_milli_c")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.odometer_meters", output_column=SchemaColumn(name="odometer_meters", type="long", nullable=True, comment="odometer_meters")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.oil_pressure_k_pa", output_column=SchemaColumn(name="oil_pressure_k_pa", type="long", nullable=True, comment="oil_pressure_k_pa")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.tire_pressure_back_left_k_pa", output_column=SchemaColumn(name="tire_pressure_back_left_k_pa", type="long", nullable=True, comment="tire_pressure_back_left_k_pa")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.tire_pressure_back_right_k_pa", output_column=SchemaColumn(name="tire_pressure_back_right_k_pa", type="long", nullable=True, comment="tire_pressure_back_right_k_pa")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.tire_pressure_front_left_k_pa", output_column=SchemaColumn(name="tire_pressure_front_left_k_pa", type="long", nullable=True, comment="tire_pressure_front_left_k_pa")),
            ValueColumnMapping(value_column="value.proto_value.engine_gauge_event.tire_pressure_front_right_k_pa", output_column=SchemaColumn(name="tire_pressure_front_right_k_pa", type="long", nullable=True, comment="tire_pressure_front_right_k_pa")),
        ],
        time_interval_bucket_ms=120087,
    ),
    # Additional Diesel Particulate Filter (DPF) metrics
    ObjectStatSource(
        table_name="kinesisstats.osdfuelconsumptionratemlperhour",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdfuelconsumptionratemlperhour", type="long", nullable=True, comment="osdfuelconsumptionratemlperhour")),
        ],
        time_interval_bucket_ms=302808,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdenginetotalfuelusedmilliliters",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdenginetotalfuelusedmilliliters", type="long", nullable=True, comment="osdenginetotalfuelusedmilliliters")),
        ],
        time_interval_bucket_ms=304872,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdengineoiltempmicroc",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdengineoiltempmicroc", type="long", nullable=True, comment="osdengineoiltempmicroc")),
        ],
        time_interval_bucket_ms=244050,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdboostpressurepa",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdboostpressurepa", type="long", nullable=True, comment="osdboostpressurepa")),
        ],
        time_interval_bucket_ms=300504,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdairinletpressurepa",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdairinletpressurepa", type="long", nullable=True, comment="osdairinletpressurepa")),
        ],
        time_interval_bucket_ms=300509,
    ),
    ObjectStatSource(
        table_name="kinesisstats.osdintakemanifoldtemperaturemicrocelsius",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdintakemanifoldtemperaturemicrocelsius", type="long", nullable=True, comment="osdintakemanifoldtemperaturemicrocelsius")),
        ],
        time_interval_bucket_ms=300504,
    ),
    # Additional engine oil pressure metrics
    ObjectStatSource(
        table_name="kinesisstats.osdengineoilleveldecipercent",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdengineoilleveldecipercent", type="long", nullable=True, comment="osdengineoilleveldecipercent")),
        ],
        time_interval_bucket_ms=300491,
    ),
    # Additional engine overheating metrics
    ObjectStatSource(
        table_name="kinesisstats.osdairinlettempmillic",
        value_columns=[
            ValueColumnMapping(value_column="value.int_value", output_column=SchemaColumn(name="osdairinlettempmillic", type="long", nullable=True, comment="osdairinlettempmillic")),
        ],
        time_interval_bucket_ms=584994,
    ),
]


@dataclass
class ObjectStatSourceWithDataframe:
    source: ObjectStatSource
    df: DataFrame
