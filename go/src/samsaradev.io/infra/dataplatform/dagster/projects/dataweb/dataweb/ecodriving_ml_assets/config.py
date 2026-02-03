"""
Configuration constants and lookup functions for eco-driving ML assets.

This module consolidates all environment-specific configuration:
- SQS queue URLs
- DynamoDB table ARNs
- Feature definitions
- Environment/role lookups
"""

from enum import Enum

from dataweb.userpkgs.constants import AWSRegion

# =============================================================================
# Environment Definitions
# =============================================================================


class Environment(str, Enum):
    """Valid environment identifiers."""

    DEV = "dev"
    PROD = "prod"
    PROD_EU = "prod-eu"


# =============================================================================
# SQS Configuration
# =============================================================================
# The job writes from the Databricks AWS account to the ML AWS account,
# so we need to specify the complete URL to access the SQS outside the account.

_SQS_QUEUE_URLS = {
    Environment.DEV: "https://sqs.us-west-2.amazonaws.com/000267950901/ecodriving-efficiency-sqs",
    Environment.PROD: "https://sqs.us-west-2.amazonaws.com/175636180121/ecodriving-efficiency-sqs-us",
    Environment.PROD_EU: "https://sqs.eu-west-1.amazonaws.com/120551827380/ecodriving-efficiency-sqs-eu",
}
# TODO: Add the SQS queue URL for Canada once it is supported.


def get_sqs_queue_url(env: str) -> str:
    """Get the SQS queue URL for the given environment."""
    try:
        return _SQS_QUEUE_URLS[Environment(env)]
    except ValueError:
        valid_envs = [e.value for e in Environment]
        raise ValueError(f"Invalid environment: {env}. Valid options: {valid_envs}")


# =============================================================================
# DynamoDB Configuration
# =============================================================================
# The job writes from the Databricks AWS account to the ML AWS account,
# so we need to specify the ARN to access the table outside the account.

_DDB_TABLE_NAMES = {
    Environment.DEV: "arn:aws:dynamodb:us-west-2:000267950901:table/ecodriving-efficiency-dev",
    Environment.PROD: "arn:aws:dynamodb:us-west-2:175636180121:table/ecodriving-efficiency-prod",
    Environment.PROD_EU: "arn:aws:dynamodb:eu-west-1:120551827380:table/ecodriving-efficiency-prod",
}
# TODO: Add the DynamoDB table name for Canada once it is supported.


def get_ddb_table_name(env: str) -> str:
    """Get the DynamoDB table ARN for the given environment."""
    try:
        return _DDB_TABLE_NAMES[Environment(env)]
    except ValueError:
        valid_envs = [e.value for e in Environment]
        raise ValueError(f"Invalid environment: {env}. Valid options: {valid_envs}")


# =============================================================================
# Feature Definitions
# =============================================================================

METADATA_COLUMNS = {
    "org_id",
    "device_id",
    "cycle_id",
    "interval_start",
    "date",
    "interval_end",
    "org_cell_id",
    "enrichment_due_at",
    "is_j1939",
    "associated_vehicle_engine_type",
}

# Feature name to numeric ID mapping for compact SQS message encoding.
# IMPORTANT: Never change existing IDs - only append new features at the end.
# The same mapping is used in the consumer to decode feature IDs back to names.
# If you need to add a new feature, add it at the end and increment the ID by 1.
FEATURE_NAME_TO_ID: dict[str, int] = {
    # Cruise/coasting metrics (1-2)
    "total_cruise_control_ms": 1,
    "total_coasting_ms": 2,
    # Weight metrics (3-5)
    "weight_time": 3,
    "weighted_weight": 4,
    "average_weight_kg": 5,
    # Grade metrics (6-7)
    "uphill_duration_ms": 6,
    "downhill_duration_ms": 7,
    # Braking metrics (8-9)
    "wear_free_braking_duration_ms": 8,
    "total_braking_duration_ms": 9,
    # Location statistics (10-21)
    "avg_altitude_meters": 10,
    "stddev_altitude_meters": 11,
    "stddev_speed_mps": 12,
    "avg_latitude": 13,
    "avg_longitude": 14,
    "altitude_delta_meters": 15,
    "earliest_heading_degrees": 16,
    "heading_change_degrees": 17,
    "heading_step_mean_absolute_degrees": 18,
    "latest_heading_degrees": 19,
    "revgeo_country": 20,
    # Speed buckets (21-27)
    "ms_in_speed_0_25mph": 21,
    "ms_in_speed_25mph_80kph": 22,
    "ms_in_speed_80kph_50mph": 23,
    "ms_in_speed_50mph_55mph": 24,
    "ms_in_speed_55mph_100kph": 25,
    "ms_in_speed_100kph_65mph": 26,
    "ms_in_speed_65_plus_mph": 27,
    # Acceleration buckets (28-32)
    "ms_in_accel_hard_braking": 28,
    "ms_in_accel_mod_braking": 29,
    "ms_in_accel_cruise_coast": 30,
    "ms_in_accel_mod_accel": 31,
    "ms_in_accel_hard_accel": 32,
    # OSM road attributes (33-37)
    "avg_lanes": 33,
    "avg_speed_limit_kph": 34,
    "road_type_diversity": 35,
    "speed_limit_stddev": 36,
    "predominant_road_type": 37,
    # Accelerator pedal (38)
    "accelerator_pedal_time_gt95_ms": 38,
    # Engine state metrics (39-40)
    "on_duration_ms": 39,
    "idle_duration_ms": 40,
    # Fuel metrics (41)
    "fuel_consumed_ml": 41,
    # Distance metrics (42)
    "distance_traveled_m": 42,
    # Vehicle information (43-48)
    "associated_vehicle_make": 43,
    "associated_vehicle_model": 44,
    "associated_vehicle_year": 45,
    "associated_vehicle_primary_fuel_type": 46,
    "associated_vehicle_gross_vehicle_weight_rating": 47,
    "associated_vehicle_engine_type": 48,
    # Organization information (49-50)
    "locale": 49,
    "last_cable_id": 50,
    # Temperature (51)
    "median_air_temp_milli_c": 51,
}

# Set of valid feature names (for backwards compatibility)
VALID_FEATURE_NAMES = set(FEATURE_NAME_TO_ID.keys())


# =============================================================================
# Environment Configuration
# =============================================================================

_ECO_DRIVING_DB_NAMES = {
    "dev": "datamodel_dev",
    "prod": "feature_store",
}

_ROLE_NAMES = {
    "dev": "ecodriving-efficiency-dev",
    "prod": "ecodriving-efficiency-prod",
}

# Maps (region, env) -> running environment
_RUNNING_ENV_MAP = {
    (AWSRegion.US_WEST_2, "dev"): Environment.DEV.value,
    (AWSRegion.US_WEST_2, "prod"): Environment.PROD.value,
    (AWSRegion.EU_WEST_1, "dev"): Environment.DEV.value,
    (AWSRegion.EU_WEST_1, "prod"): Environment.PROD_EU.value,
}


def get_eco_driving_features_db_name(env: str) -> str:
    """Get the database name for eco-driving features based on environment."""
    if env not in _ECO_DRIVING_DB_NAMES:
        valid_envs = list(_ECO_DRIVING_DB_NAMES.keys())
        raise ValueError(f"Invalid environment: {env}. Valid options: {valid_envs}")
    return _ECO_DRIVING_DB_NAMES[env]


def get_running_env(env: str, region: str) -> str:
    """
    Get the running environment identifier based on env and region.

    Args:
        env: The deployment environment ('dev' or 'prod')
        region: The AWS region

    Returns:
        The running environment string (e.g., 'dev', 'prod', 'prod-eu')
    """
    if region == AWSRegion.CA_CENTRAL_1:
        raise ValueError("Canada region is not yet supported.")

    key = (region, env)
    if key not in _RUNNING_ENV_MAP:
        raise ValueError(
            f"Invalid region/environment combination: region={region}, env={env}"
        )
    return _RUNNING_ENV_MAP[key]


def get_role_name(env: str) -> str:
    """Get the IAM role name for the given environment."""
    if env not in _ROLE_NAMES:
        valid_envs = list(_ROLE_NAMES.keys())
        raise ValueError(f"Invalid environment: {env}. Valid options: {valid_envs}")
    return _ROLE_NAMES[env]
