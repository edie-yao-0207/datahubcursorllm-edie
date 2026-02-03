from dataclasses import dataclass
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
from dataweb.userpkgs.firmware.constants import EngineType, Signal
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@dataclass
class Market:
    US = "US"
    EMEA = "EMEA"
    MX = "MX"


class TableColumn(Enum):
    def __str__(self):
        return str(self.value)

    SIGNAL_NAME = Column(
        name="signal_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Name of the signal to prioritize."),
    )
    PRIORITY = Column(
        name="priority",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(comment="Product priority for the signal."),
    )


SCHEMA = columns_to_schema(
    ColumnType.MARKET,
    ColumnType.ENGINE_TYPE,
    TableColumn.SIGNAL_NAME,
    TableColumn.PRIORITY,
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.MARKET,
    ColumnType.ENGINE_TYPE,
    TableColumn.SIGNAL_NAME,
)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")


def emea_ice_priorities() -> list:
    return [
        (Market.EMEA, EngineType.ICE, Signal.ENGINE_STATE, 0),
        (Market.EMEA, EngineType.ICE, Signal.ENGINE_FAULTS, 0),
        (Market.EMEA, EngineType.ICE, Signal.FUEL_CONSUMPTION, 0),
        (Market.EMEA, EngineType.ICE, Signal.FUEL_LEVEL, 0),
        (Market.EMEA, EngineType.ICE, Signal.ODOMETER, 0),
    ]


def us_ice_priorities() -> list:
    return [
        (Market.US, EngineType.ICE, Signal.ENGINE_STATE, 0),
        (Market.US, EngineType.ICE, Signal.FUEL_CONSUMPTION, 0),
        (Market.US, EngineType.ICE, Signal.FUEL_LEVEL, 0),
        (Market.US, EngineType.ICE, Signal.ODOMETER, 0),
        (Market.US, EngineType.ICE, Signal.TOTAL_ENGINE_RUNTIME, 0),
    ]


def mx_ice_priorities() -> list:
    return [
        (Market.MX, EngineType.ICE, Signal.ENGINE_STATE, 0),
        (Market.MX, EngineType.ICE, Signal.ENGINE_FAULTS, 0),
        (Market.MX, EngineType.ICE, Signal.FUEL_CONSUMPTION, 0),
    ]


def ev_hybrid_priorities(market: Market) -> list:
    """
    ev_hybrid_priorities returns the list of signals to prioritize for EV and Hybrid vehicles. BEV, PHEV, and HYBRID
    vehicles all have the same requirements in every market.
    """

    return [
        (market, EngineType.BEV, Signal.ENGINE_STATE, 0),
        (market, EngineType.BEV, Signal.CHARGING_STATUS, 0),
        (market, EngineType.BEV, Signal.STATE_OF_CHARGE, 0),
        (market, EngineType.BEV, Signal.CONSUMED_ENERGY, 0),
        (market, EngineType.BEV, Signal.BATTERY_STATE_OF_HEALTH, 0),
        (market, EngineType.BEV, Signal.DISTANCE_DRIVEN_ON_ELECTRIC, 0),
        (market, EngineType.PHEV, Signal.ENGINE_STATE, 0),
        (market, EngineType.PHEV, Signal.STATE_OF_CHARGE, 0),
        (market, EngineType.HYBRID, Signal.ENGINE_STATE, 0),
        (market, EngineType.HYBRID, Signal.STATE_OF_CHARGE, 0),
    ]


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Prioritization of signals for telematics products. Prorities are based on feedback from Telematics product.",
        row_meaning="Signals to prioritize for telematics products.",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_MARKET_METADATA.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
)
def fct_telematics_market_metadata(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    return spark.createDataFrame(
        schema=StructType(
            [
                StructField(str(ColumnType.MARKET), StringType(), True),
                StructField(str(ColumnType.ENGINE_TYPE), StringType(), True),
                StructField(str(TableColumn.SIGNAL_NAME), StringType(), True),
                StructField(str(TableColumn.PRIORITY), IntegerType(), True),
            ]
        ),
        data=sum(
            [
                emea_ice_priorities(),
                us_ice_priorities(),
                mx_ice_priorities(),
                ev_hybrid_priorities(Market.EMEA),
                ev_hybrid_priorities(Market.US),
                ev_hybrid_priorities(Market.MX),
            ],
            [],
        ),
    )
