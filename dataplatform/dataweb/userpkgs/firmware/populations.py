from copy import deepcopy
from enum import Enum
from typing import List

from dataweb.userpkgs.firmware.schema import ColumnType, columns_to_names


class Population(Enum):
    def __post_init__(self) -> None:
        """
        Get all populations in the Population class. Assert that all dimensions are in the DEVICE dimension.
        """
        most_granular = columns_to_names(Population.MOST_GRANULAR)

        for population in Population:
            for dimension in population:
                assert (
                    dimension in most_granular
                ), f"Dimension {dimension} not in Population.DEVICE"

    def __str__(self) -> str:
        return " ".join(columns_to_names(*self.value))

    @property
    def sql_columns(self) -> str:
        return ", ".join(columns_to_names(*self.value))

    ALL = []
    MOST_GRANULAR = [
        # If a new dimension is added to any grouping, also add it here
        ColumnType.ORG_ID,
        ColumnType.GATEWAY_ID,
        ColumnType.MARKET,
        ColumnType.QUARANTINE_ENABLED,
        ColumnType.INTERNAL_TYPE_NAME,
        ColumnType.BUILD,
        ColumnType.PRODUCT_GROUP_ID,
        ColumnType.PRODUCT_PROGRAM_ID,
        ColumnType.ROLL_OUT_STAGE_ID,
        ColumnType.PRODUCT_NAME,
        ColumnType.VARIANT_NAME,
        ColumnType.CABLE_NAME,
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
        ColumnType.FUEL_TYPE,
        ColumnType.ENGINE_TYPE,
        ColumnType.ASSET_TYPE_NAME,
        ColumnType.ASSET_TYPE_SEGMENT,
        ColumnType.ASSET_SUBTYPE,
    ]
    ORG = [ColumnType.ORG_ID]
    BUILD_ROLLOUT_STAGE = [
        ColumnType.BUILD,
        ColumnType.ROLL_OUT_STAGE_ID,
        ColumnType.PRODUCT_NAME,
        ColumnType.PRODUCT_PROGRAM_ID,
    ]
    BUILD_PRODUCT_ID = [ColumnType.BUILD, ColumnType.PRODUCT_NAME]
    MAKE_MODEL_YEAR = [ColumnType.MAKE, ColumnType.MODEL, ColumnType.YEAR]
    MAKE_MODEL_YEAR_ENGINE_MODEL = [
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
    ]
    MAKE_MODEL_YEAR_ENGINE_MODEL_ENGINE_TYPE = [
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
        ColumnType.ENGINE_TYPE,
    ]
    MAKE_MODEL_YEAR_ENGINE_MODEL_ENGINE_TYPE_FUEL_TYPE = [
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
        ColumnType.ENGINE_TYPE,
        ColumnType.FUEL_TYPE,
    ]
    PRODUCT_DEVICE_MMYEF = [
        ColumnType.PRODUCT_NAME,
        ColumnType.CABLE_NAME,
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
        ColumnType.ENGINE_TYPE,
        ColumnType.FUEL_TYPE,
    ]
    PRODUCT_VARIANT_MMYEF = [
        ColumnType.VARIANT_NAME,
        ColumnType.CABLE_NAME,
        ColumnType.MAKE,
        ColumnType.MODEL,
        ColumnType.YEAR,
        ColumnType.ENGINE_MODEL,
        ColumnType.ENGINE_TYPE,
        ColumnType.FUEL_TYPE,
    ]
    MARKET_PRODUCT_CABLE = [
        ColumnType.MARKET,
        ColumnType.PRODUCT_NAME,
        ColumnType.CABLE_NAME,
    ]
    MARKET_ENGINE_TYPE = [ColumnType.MARKET, ColumnType.ENGINE_TYPE]
    MARKET_FUEL_ENGINE_TYPE = [
        ColumnType.MARKET,
        ColumnType.FUEL_TYPE,
        ColumnType.ENGINE_TYPE,
    ]
    PRODUCT_NAME = [ColumnType.PRODUCT_NAME]
    PRODUCT_STAGE_PROGRAM = [
        ColumnType.PRODUCT_NAME,
        ColumnType.ROLL_OUT_STAGE_ID,
        ColumnType.PRODUCT_PROGRAM_ID,
    ]
    ASSET_TYPE = [
        ColumnType.ASSET_TYPE_NAME,
    ]
    ASSET_TYPE_SEGMENT = [
        ColumnType.ASSET_TYPE_NAME,
        ColumnType.ASSET_TYPE_SEGMENT,
    ]
    ASSET_TYPE_SUBTYPE = [
        ColumnType.ASSET_TYPE_NAME,
        ColumnType.ASSET_TYPE_SEGMENT,
        ColumnType.ASSET_SUBTYPE,
    ]
    ENGINE_TYPE_PROGRAM = [
        ColumnType.ENGINE_TYPE,
        ColumnType.ROLL_OUT_STAGE_ID,  # Not used for the population, but useful for filtering.
        ColumnType.PRODUCT_PROGRAM_ID,
    ]


SQL_COLUMNS = [population.sql_columns for population in Population]
GROUPING_SETS = ", ".join([f"({columns})" for columns in SQL_COLUMNS])

DIMENSIONS = Population.MOST_GRANULAR.value
GROUPINGS: List[List[ColumnType]] = [e.value for e in Population]


def population_id_value_expr(grouping_id_expr: str, population: Population) -> str:
    """Gets an SQL string to derive a population ID from a set of dimension columns. A population
    ID is the MD5 hash of all the relevant columns to a population.
    """
    column_names = [str(col) for col in population.value]

    dimension_columns_casted = [
        f"CAST(COALESCE({col_name}, 'null') AS STRING)" for col_name in column_names
    ]
    return f"MD5(CONCAT(CAST({grouping_id_expr} AS STRING), { ', '.join(dimension_columns_casted) }))"


DEVICE_COLUMNS = Population.MOST_GRANULAR.sql_columns

NULLABLE_DIMENSION_COLUMNS = [
    (lambda c: setattr(c, "nullable", True) or c)(deepcopy(col_enum.value))
    for col_enum in Population.MOST_GRANULAR.value
]
