from dataclasses import dataclass, field

from dagster import DailyPartitionsDefinition

PARTITION_DATE_START = "2024-01-01"
DATAWEB_PARTITION_DATE = DailyPartitionsDefinition(start_date=PARTITION_DATE_START)


@dataclass
class ConcurrencyKey:
    """Indicates priority for tables which queue large batches of runs"""

    VDP = "VDP"


@dataclass
class ProductArea:
    VEHICLE_DIAGNOSTICS = "Vehicle Diagnostics"
    MATERIAL_SPREADER = "Material Spreader"
    MAINTENANCE = "Maintenance"
    ELECTRIC_VEHICLES = "Electric Vehicles"
    US_COMPLIANCE = "US Compliance"
    FUEL_AND_ENERGY = "Fuel and Energy"
    ECO_DRIVING = "Eco Driving"
    GENERAL_INFORMATION = "General Information"
    REEFERS = "Refridgeration Units"
    EU_COMPLIANCE = "EU Compliance"
    INTERNAL = "Internal"
    INDUSTRIAL = "Industrial"
    DIMENSIONS = "Device Dimensions"
    TELEMATICS_PLATFORM = "Telematics Platform"
    TIRE_PRESSURE_MONITORING = "Tire Pressure Monitoring"
    GPS = "GPS"
    SAFETY = "Safety"


@dataclass
class Signal:
    ENGINE_STATE = "Engine State"
    ENGINE_ACTIVITY_METHOD = "Engine Activity Method"
    STATE_OF_CHARGE = "State of Charge"
    ODOMETER = "Odometer"
    TOTAL_ENGINE_RUNTIME = "Total Engine Runtime"
    FUEL_CONSUMPTION = "Fuel Consumption"
    ENGINE_FAULTS = "Engine Faults"
    CHARGING_STATUS = "Charging Status"
    CONSUMED_ENERGY = "Consumed Energy"
    BATTERY_STATE_OF_HEALTH = "Battery State of Health"
    DISTANCE_DRIVEN_ON_ELECTRIC = "Distance Driven on Electric"
    ENGINE_TORQUE_OVER_LIMIT = "Engine Torque Over Limit"
    COASTING = "Coasting"
    SMALL_BATTERY_VOLTAGE = "12V Battery Voltage"
    FUEL_LEVEL = "Fuel Level"
    FUEL_LEVEL_2 = "Fuel Level 2"


@dataclass
class ScalarAggregation:
    """Numeric aggregations done to objectStats, excluding array values like histogram and percentile."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    STDDEV = "stddev"
    VARIANCE = "variance"
    MEAN = "mean"
    MEDIAN = "median"
    MODE = "mode"
    FIRST = "first"
    LAST = "last"
    KURTOSIS = "kurtosis"
    MIN = "min"
    MAX = "max"


@dataclass
class EngineType:
    """
    The Canonical Fuel Type defines an Engine Type. The Engine Type is the major
    cateogry of the Fuel Type.
    """

    BEV = "BEV"
    ICE = "ICE"
    PHEV = "PHEV"
    HYBRID = "HYBRID"
    HYDROGEN = "HYDROGEN"


@dataclass
class FuelType:
    """
    The Canonical Fuel Type
    """

    GASOLINE_STANDARD: str = field(default="Gasoline_Standard")
    DIESEL_STANDARD: str = field(default="Diesel_Standard")


@dataclass
class SourceLink:
    ELECTRIC_VEHICLES = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5695980/VDP+Electric+Vehicles"
    ENGINE_STATE = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5279935/VDP+Engine+State"
    FUEL_CONSUMPTION = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4621773/VDP+Fuel+Consumption"
    ECO_DRIVING = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5818493/VDP+EcoDriving"
    ODOMETER = (
        "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5715439/VDP+Odometer"
    )
    POWER_TAKE_OFF = (
        "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5773902/PTO+Status"
    )
    MATERIAL_SPREADER = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5883646/VDP+Material+Spreader"
    TACHOGRAPH = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4621773/VDP+Fuel+Consumption"
