# Databricks notebook source
"""
Commercial Speed Limits Builder for Databricks Notebooks

This module provides the build_commercial_speed_limit_map function and related utilities
for processing commercial speed limits in Databricks notebooks. The function and constants
are copied from the main backend repository to avoid dependency issues.
"""

# COMMAND ----------

import json
from typing import Dict


# Vehicle Type Tag Constants (copied from vehicle_type_tags_pb2)
# These correspond to the proto enum values in samsaradev.io/hubproto/maptileproto/vehicle_type_tags.proto
INVALID = 0
DEFAULT = 1  # Passenger/Car
BUS = 2  # All bus types e.g. public, private, school buses etc ...

# Vehicle types in metric tons designed for Europe
GOODS_VEHICLE_NO_MAX_LADEN = 23  # No max laden weight available
GOODS_VEHICLE_2_T = 24  # < 2 metric tons
GOODS_VEHICLE_3_5_T = 25  # >= 2 metric tons && < 3.5 metric tons
GOODS_VEHICLE_7_5_T = 26  # >= 3.5 metric tons && < 7.5 metric tons
GOODS_VEHICLE_12_T = 27  # >= 7.5 metric tons && < 12 metric tons
GOODS_VEHICLE_MORE_THAN_12_T = 28  # >= 12 metric tons

# Vehicle types in pounds designed for US
GOODS_VEHICLE_10000_LBS = 29  # < 10000 lbs
GOODS_VEHICLE_26000_LBS = 30  # >= 10000 lbs && < 26000 lbs
GOODS_VEHICLE_MORE_THAN_26000_LBS = 31  # >= 26000 lbs

# Vehicle type for the passenger car - to be used with the new Map editor version with CSL enabled
PASSENGER = 32  # Passenger/Car
GOODS_VEHICLE_3_5_T_EXACT = 33  # Exactly 3.5 metric tons

# TomTom Related Constants
TOMTOM_CAR = 0
TOMTOM_BUS = 17
TOMTOM_ALL_TRUCKS = 50
TOMTOM_STRAIGHT_TRUCK = 51  # also known as a medium truck
TOMTOM_TRACTOR_TRAILER = 52  # also known as a heavy truck

# View Table in RFC for more details: https://paper.dropbox.com/doc/RFC-Vehicle-Classification-for-Commercial-Speed-Limits-May-2021--BK6WGM68IwvJ72jS2fIA3O2IAg-N4J2IU0J6NjsbyPuSr20d
# For US state specific vehicle types info, Appendix of the CSL PRD: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5420307/PRD+Commercial+Speed+Limits
SPEED_LIMIT_MATRIX = {
    "GBR": {
        TOMTOM_CAR: [DEFAULT, GOODS_VEHICLE_2_T],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [
            GOODS_VEHICLE_NO_MAX_LADEN,
            GOODS_VEHICLE_3_5_T,
            GOODS_VEHICLE_7_5_T,
        ],
        TOMTOM_TRACTOR_TRAILER: [GOODS_VEHICLE_MORE_THAN_12_T, GOODS_VEHICLE_12_T],
    },
    "FRA": {
        TOMTOM_CAR: [
            DEFAULT,
            GOODS_VEHICLE_2_T,
            GOODS_VEHICLE_3_5_T,
        ],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [
            GOODS_VEHICLE_NO_MAX_LADEN,
            GOODS_VEHICLE_7_5_T,
            GOODS_VEHICLE_12_T,
        ],
        TOMTOM_TRACTOR_TRAILER: [GOODS_VEHICLE_MORE_THAN_12_T],
    },
    "DEU": {
        TOMTOM_CAR: [DEFAULT, GOODS_VEHICLE_2_T, GOODS_VEHICLE_3_5_T],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [GOODS_VEHICLE_NO_MAX_LADEN, GOODS_VEHICLE_7_5_T],
        TOMTOM_TRACTOR_TRAILER: [GOODS_VEHICLE_12_T, GOODS_VEHICLE_MORE_THAN_12_T],
    },
    "IRL": {
        TOMTOM_CAR: [
            DEFAULT,
            GOODS_VEHICLE_2_T,
            GOODS_VEHICLE_3_5_T,
            GOODS_VEHICLE_3_5_T_EXACT,
        ],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [
            GOODS_VEHICLE_NO_MAX_LADEN,
            GOODS_VEHICLE_7_5_T,
            GOODS_VEHICLE_12_T,
            GOODS_VEHICLE_MORE_THAN_12_T,
        ],
        TOMTOM_TRACTOR_TRAILER: [],
    },
    "ESP": {
        TOMTOM_CAR: [DEFAULT, GOODS_VEHICLE_2_T, GOODS_VEHICLE_3_5_T],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [
            GOODS_VEHICLE_NO_MAX_LADEN,
            GOODS_VEHICLE_7_5_T,
            GOODS_VEHICLE_12_T,
            GOODS_VEHICLE_MORE_THAN_12_T,
        ],
        TOMTOM_TRACTOR_TRAILER: [],
    },
    "USA": {
        # To be used for all US states that are not listed below
        "DEFAULT": {
            TOMTOM_CAR: [DEFAULT],
            TOMTOM_BUS: [BUS],
            TOMTOM_STRAIGHT_TRUCK: [
                GOODS_VEHICLE_NO_MAX_LADEN,
                GOODS_VEHICLE_10000_LBS,
                GOODS_VEHICLE_26000_LBS,
                GOODS_VEHICLE_MORE_THAN_26000_LBS,
            ],
            TOMTOM_TRACTOR_TRAILER: [],
        },
        # Alabama State
        "AL": {
            TOMTOM_CAR: [DEFAULT, GOODS_VEHICLE_10000_LBS],
            TOMTOM_BUS: [BUS],
            TOMTOM_STRAIGHT_TRUCK: [
                GOODS_VEHICLE_NO_MAX_LADEN,
                GOODS_VEHICLE_26000_LBS,
                GOODS_VEHICLE_MORE_THAN_26000_LBS,
            ],
            TOMTOM_TRACTOR_TRAILER: [],
        },
        # California State
        "CA": {
            TOMTOM_CAR: [
                DEFAULT,
                GOODS_VEHICLE_10000_LBS,
                GOODS_VEHICLE_26000_LBS,
            ],
            TOMTOM_BUS: [BUS],
            TOMTOM_STRAIGHT_TRUCK: [
                GOODS_VEHICLE_NO_MAX_LADEN,
                GOODS_VEHICLE_MORE_THAN_26000_LBS,
            ],
            TOMTOM_TRACTOR_TRAILER: [],
        },
        # Oregon State
        "OR": {
            TOMTOM_CAR: [DEFAULT],
            TOMTOM_BUS: [BUS],
            TOMTOM_STRAIGHT_TRUCK: [
                GOODS_VEHICLE_NO_MAX_LADEN,
                GOODS_VEHICLE_10000_LBS,
                GOODS_VEHICLE_26000_LBS,
                GOODS_VEHICLE_MORE_THAN_26000_LBS,
            ],
            TOMTOM_TRACTOR_TRAILER: [],
        },
    },
    "DEFAULT": {
        TOMTOM_CAR: [DEFAULT],
        TOMTOM_BUS: [BUS],
        TOMTOM_STRAIGHT_TRUCK: [
            GOODS_VEHICLE_NO_MAX_LADEN,
            GOODS_VEHICLE_2_T,
            GOODS_VEHICLE_3_5_T,
            GOODS_VEHICLE_7_5_T,
            GOODS_VEHICLE_12_T,
            GOODS_VEHICLE_MORE_THAN_12_T,
        ],
        TOMTOM_TRACTOR_TRAILER: [],
    },
}

# Country mappings for regions that use the same vehicle categorization
SPEED_LIMIT_MATRIX["ITA"] = SPEED_LIMIT_MATRIX["FRA"]
SPEED_LIMIT_MATRIX["BEL"] = SPEED_LIMIT_MATRIX["DEU"]
SPEED_LIMIT_MATRIX["AUT"] = SPEED_LIMIT_MATRIX["ESP"]
SPEED_LIMIT_MATRIX["LUX"] = SPEED_LIMIT_MATRIX["ESP"]
SPEED_LIMIT_MATRIX["NLD"] = SPEED_LIMIT_MATRIX["ESP"]
# We want to use the same vehicle categorization as US for Canada to provide
# smoother North America experience for our customers
SPEED_LIMIT_MATRIX["CAN"] = SPEED_LIMIT_MATRIX["USA"]["DEFAULT"]
# We want to use the same vehicle categorization as US for Mexico to provide
# smoother North America experience for our customers
SPEED_LIMIT_MATRIX["MEX"] = SPEED_LIMIT_MATRIX["USA"]["DEFAULT"]
# Nevada State has the same Vehicle type definition as Alabama State
SPEED_LIMIT_MATRIX["USA"]["NV"] = SPEED_LIMIT_MATRIX["USA"]["AL"]
# Arkansas State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["AR"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Idaho State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["ID"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Indiana State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["IN"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Michigan State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["MI"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Montana State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["MT"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Ohio State has the same Vehicle type definition as California State
SPEED_LIMIT_MATRIX["USA"]["OH"] = SPEED_LIMIT_MATRIX["USA"]["CA"]
# Washington State has the same Vehicle type definition as Oregon State
SPEED_LIMIT_MATRIX["USA"]["WA"] = SPEED_LIMIT_MATRIX["USA"]["OR"]


# COMMAND ----------


def convert_unitized_speed_limit_to_kmph(speed_limit: str) -> float:
    """
    Convert a unitized speed limit string to kmph.

    Args:
        speed_limit: Speed limit string in format "60 mph" or "60 kph"

    Returns:
        Speed limit in kmph as float

    Raises:
        MalformedSpeedLimit: If the speed limit format is invalid
    """
    MPH_TO_KPH = 1.6093

    mph_index = speed_limit.find(" mph")
    kph_index = speed_limit.find(" kph")
    milliknots_index = speed_limit.find(" milliknots")
    if milliknots_index > -1:
        speed_limit = int(speed_limit[0:milliknots_index]) * 0.001852
    elif mph_index > -1:
        # must be in mph so convert to kmph
        speed_limit = int(speed_limit[0:mph_index]) * MPH_TO_KPH
    elif kph_index > -1:
        speed_limit = int(speed_limit[0:kph_index])
    else:
        raise MalformedSpeedLimit(
            "The speed limit is malformed! {0}".format(speed_limit)
        )

    return float(round(speed_limit, 2))


def build_commercial_speed_limit_map(
    passenger_speed_limit: str,
    commercial_speed_limit_map: str,
    country_code: str,
    state_code: str,
    include_passenger_speed_limit: bool = True,
    skip_passenger_speed_comparison: bool = False,
) -> Dict[str, float]:
    """
    Build a commercial speed limit map from TomTom data.

    This function takes TomTom commercial speed limit data and converts it to
    Samsara vehicle type speed limits based on country/state regulations.

    Args:
        passenger_speed_limit: Passenger vehicle speed limit (e.g., "60 mph")
        commercial_speed_limit_map: JSON string containing TomTom commercial speed limits
        country_code: Country code (e.g., "USA", "GBR", "FRA")
        state_code: State code for US states (e.g., "CA", "TX") or None for other countries
        include_passenger_speed_limit: Whether to include passenger speed limit in output
        skip_passenger_speed_comparison: When True, include all CSL speed limits even if
            they match the passenger speed limit. Useful for backend CSV generation.

    Returns:
        Dictionary mapping vehicle type strings to speed limits in kmph

    Example:
        >>> build_commercial_speed_limit_map(
        ...     "60 mph",
        ...     '{"50": "45 mph", "51": "40 mph"}',
        ...     "USA",
        ...     "CA"
        ... )
        {'29': 72.42, '30': 64.37}
    """
    speed_map = {}
    # if the country code is not supported, we use the default
    if country_code in SPEED_LIMIT_MATRIX:
        if country_code == "USA":
            # if the state code is not supported, we use the default for USA - no CSL support
            if state_code in SPEED_LIMIT_MATRIX[country_code]:
                country_tomtom_to_samsara = SPEED_LIMIT_MATRIX[country_code][state_code]
            else:
                country_tomtom_to_samsara = SPEED_LIMIT_MATRIX[country_code]["DEFAULT"]
        else:
            country_tomtom_to_samsara = SPEED_LIMIT_MATRIX[country_code]
    else:
        country_tomtom_to_samsara = SPEED_LIMIT_MATRIX["DEFAULT"]

    try:
        commercial_speed_limit_map = json.loads(commercial_speed_limit_map)
        if commercial_speed_limit_map is None:
            return {}
    except json.decoder.JSONDecodeError:
        return {}

    # the commercial_speed_limit_map doesn't contain passenger speed limits
    # so we add it in here (optional)
    if include_passenger_speed_limit:
        commercial_speed_limit_map[TOMTOM_CAR] = passenger_speed_limit

    converted_passenger_speed_limit = convert_unitized_speed_limit_to_kmph(
        passenger_speed_limit
    )
    for tomtom_vehicle_type, speed_limit in commercial_speed_limit_map.items():
        tomtom_vehicle_type = int(tomtom_vehicle_type)
        samsara_vehicle_types = []
        if tomtom_vehicle_type == TOMTOM_ALL_TRUCKS:
            samsara_vehicle_types = (
                country_tomtom_to_samsara[TOMTOM_STRAIGHT_TRUCK]
                + country_tomtom_to_samsara[TOMTOM_TRACTOR_TRAILER]
            )
        elif tomtom_vehicle_type in country_tomtom_to_samsara:
            samsara_vehicle_types = country_tomtom_to_samsara[tomtom_vehicle_type]
        else:
            print(
                "The TomTom Vehicle Type {0} is not supported for {1}".format(
                    tomtom_vehicle_type, country_code
                )
            )

        for samsara_vehicle_type in samsara_vehicle_types:
            # When skip_passenger_speed_comparison is True, always include the speed limit.
            # Otherwise, skip vehicle types when speed limit is the same as the passenger speed limit,
            # with the exception of GOODS_VEHICLE_3_5_T_EXACT that tackles an edge case in Ireland.
            should_include = (
                skip_passenger_speed_comparison
                or converted_passenger_speed_limit
                != convert_unitized_speed_limit_to_kmph(speed_limit)
                or samsara_vehicle_type == GOODS_VEHICLE_3_5_T_EXACT
            )
            if should_include:
                speed_map[
                    str(samsara_vehicle_type)
                ] = convert_unitized_speed_limit_to_kmph(speed_limit)

    return speed_map


class MalformedSpeedLimit(Exception):
    """Exception raised when a speed limit string is malformed."""

    pass
