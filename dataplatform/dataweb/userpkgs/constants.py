from dagster import AssetKey
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

GENERAL_PURPOSE_INSTANCE_POOL_KEY = "GENERAL_PURPOSE"
MEMORY_OPTIMIZED_INSTANCE_POOL_KEY = "MEMORY_OPTIMIZED"
SLACK_ALERTS_CHANNEL_DATA_ENGINEERING = "alerts-data-engineering"

PRIMARY_KEYS_DATE_ORG_DEVICE = ["date", "org_id", "device_id"]

FRESHNESS_SLO_9AM_PST = "9am PST"
FRESHNESS_SLO_9PM_PST = "9pm PST"
FRESHNESS_SLO_12PM_PST = "12pm PST"

# Define industry vertical mappings
INDUSTRY_VERTICALS = {
    "Transportation, Wholesale & Retail Trade, Manufacturing": [
        "Transportation & Warehousing",
        "Wholesale Trade",
        "Passenger Transit",
        "Manufacturing",
        "Food & Beverage",
        "Retail Trade",
        "Consumer Products",
        "Remote Infrastructure",
    ],
    "Field Services, Construction, Utilities & Energy": [
        "Mining, Quarrying, Oil & Gas",
        "Mining, Quarrying, Oil and Gas",
        "Construction",
        "Extraction",
        "Field Services",
        "Utilities",
    ],
    "Government, Education, Healthcare": [
        "Government",
        "Educational Services",
        "Health Care & Social Assistance",
    ],
}

GLOBAL_PUBSEC_QUERY = """
pubsec_us AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM finopsdb.customer_info
),
pubsec_eu AS (
    -- This identifies whether an organization is PubSec or not
    -- Note that we only care about this for US accounts
    SELECT
        sam_number,
        sled_account
    FROM delta.`s3://samsara-eu-rds-delta-lake/table-parquet/prod-finopsdb/finopsdb/customer_info_v0`
),"""

GLOBAL_ARR_QUERY = """
account_arr AS (
    -- Handles edge case where multiple accounts with different ARRs exist for a given SAM
    SELECT
        account_id,
        customer_arr,
        customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
),
account_arr_sam AS (
    -- Handles cases where there's no account_id match (take max for SAM instead)
    SELECT sam_number_undecorated AS sam_number,
    MAX_BY(customer_arr, customer_arr) AS customer_arr,
    MAX_BY(customer_arr_segment, customer_arr) AS customer_arr_segment
    FROM product_analytics_staging.stg_customer_enriched
    WHERE date = (SELECT MAX(date) FROM product_analytics_staging.stg_customer_enriched)
    GROUP BY sam_number_undecorated
),
"""

ACCOUNT_BILLING_COUNTRY = """
COALESCE(CASE WHEN o.account_billing_country IN ('United Kingdom', 'Ireland', 'Isle of Man') THEN 'UK&I'
WHEN o.account_billing_country IN ('Germany', 'Austria', 'Switzerland') THEN 'DACH'
WHEN o.account_billing_country IN ('Belgium', 'Netherlands', 'Luxembourg') THEN 'BNL'
WHEN o.account_billing_country IN ('Bulgaria', 'Czech Republic', 'Denmark', 'France', 'Hungary', 'Italy', 'Poland', 'Romania', 'Slovakia', 'Spain', 'Sweden') THEN 'Mainland Europe: Others'
WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 1 THEN 'United States PubSec'
WHEN o.account_billing_country = 'United States' AND COALESCE(p.sled_account, 0) = 0 THEN 'United States Private'
WHEN o.account_billing_country IN ('Canada', 'Mexico') THEN o.account_billing_country
WHEN o.account_billing_country IS NULL THEN NULL
ELSE 'Other' END, 'Unknown') AS account_billing_country,
"""


@dataclass
class MetricExpressionType:
    SUM: str = field(default="sum")
    AVG: str = field(default="avg")
    COUNT: str = field(default="count")
    MIN: str = field(default="min")
    MAX: str = field(default="max")
    STDDEV: str = field(default="stddev")
    VARIANCE: str = field(default="variance")
    PERCENT: str = field(default="percent")
    MEDIAN: str = field(default="median")
    RATIO: str = field(default="ratio")


@dataclass
class Owner:
    team: str


DATAENGINEERING = Owner("DataEngineering")
DATAANALYTICS = Owner("DataAnalytics")
DATATOOLS = Owner("DataTools")
FIRMWAREVDP = Owner("FirmwareVDP")
SUSTAINABILITY = Owner("Sustainability")


@dataclass
class AWSRegion:
    US_WEST_2: str = field(default="us-west-2")
    EU_WEST_1: str = field(default="eu-west-1")
    CA_CENTRAL_1: str = field(default="ca-central-1")


ALL_COMPUTE_REGIONS = [AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1, AWSRegion.CA_CENTRAL_1]


@dataclass
class RunEnvironment:
    DEV: str = field(default="dev")
    PROD: str = field(default="prod")


@dataclass
class WarehouseWriteMode:
    MERGE = "merge"
    OVERWRITE = "overwrite"


@dataclass
class TableType:
    DAILY_DIMENSION = "daily_dimension"
    SLOWLY_CHANGING_DIMENSION = "slowly_changing_dimension"
    TRANSACTIONAL_FACT = "transactional_fact"
    ACCUMULATING_SNAPSHOT_FACT = "accumulating_snapshot_fact"
    LOOKUP = "lookup"
    LIFETIME = "lifetime"
    AGG = "agg"
    MONTHLY_REPORTING_AGG = "monthly_reporting_agg"
    STAGING = "staging"


@dataclass
class Database:
    AUDITLOG: str = field(default="auditlog")
    PRODUCT_ANALYTICS: str = field(default="product_analytics")
    PRODUCT_ANALYTICS_STAGING: str = field(default="product_analytics_staging")
    DATAENGINEERING: str = field(default="dataengineering")
    DATAMODEL_PLATFORM: str = field(default="datamodel_platform")
    DATAMODEL_PLATFORM_BRONZE: str = field(default="datamodel_platform_bronze")
    DATAMODEL_PLATFORM_SILVER: str = field(default="datamodel_platform_silver")
    DATAMODEL_TELEMATICS: str = field(default="datamodel_telematics")
    DATAMODEL_TELEMATICS_BRONZE: str = field(default="datamodel_telematics_bronze")
    DATAMODEL_TELEMATICS_SILVER: str = field(default="datamodel_telematics_silver")
    DATAMODEL_CORE: str = field(default="datamodel_core")
    DATAMODEL_CORE_BRONZE: str = field(default="datamodel_core_bronze")
    DATAMODEL_CORE_SILVER: str = field(default="datamodel_core_silver")
    DATAMODEL_SAFETY: str = field(default="datamodel_safety")
    DATAMODEL_SAFETY_BRONZE: str = field(default="datamodel_safety_bronze")
    DATAMODEL_SAFETY_SILVER: str = field(default="datamodel_safety_silver")
    DATAMODEL_DEV: str = field(default="datamodel_dev")
    MIXPANEL_SAMSARA: str = field(default="mixpanel_samsara")
    FIRMWARE_DEV: str = field(default="firmware_dev")
    KINESISSTATS: str = field(default="kinesisstats")
    KINESISSTATS_HISTORY: str = field(default="kinesisstats_history")
    DEFINITIONS: str = field(default="definitions")
    DATAPREP: str = field(default="dataprep")
    DATAPREP_FIRMWARE: str = field(default="dataprep_firmware")
    PRODUCTS_DB: str = field(default="productsdb")
    CLOUD_DB: str = field(default="clouddb")
    FUELDB_SHARDS: str = field(default="fueldb_shards")
    SIGNALPROMOTIONDB: str = field(default="signalpromotiondb")
    FEATURE_STORE: str = field(default="feature_store")
    INFERENCE_STORE: str = field(default="inference_store")
    ATTRIBUTEDB_SHARDS: str = field(default="attributedb_shards")
    DYNAMODB: str = field(default="dynamodb")
    DATAMODEL_LAUNCHDARKLY_BRONZE: str = field(default="datamodel_launchdarkly_bronze")
    DOJO: str = field(default="dojo")
    DATAMODEL_LAUNCHDARKLY_SILVER: str = field(default="datamodel_launchdarkly_silver")
    SCORINGDB_SHARDS: str = field(default="scoringdb_shards")


@dataclass
class InstanceType:
    MD_FLEET_8XLARGE: str = field(default="md-fleet.8xlarge")
    MD_FLEET_4XLARGE: str = field(default="md-fleet.4xlarge")
    MD_FLEET_2XLARGE: str = field(default="md-fleet.2xlarge")
    MD_FLEET_XLARGE: str = field(default="md-fleet.xlarge")
    RD_FLEET_8XLARGE: str = field(default="rd-fleet.8xlarge")
    RD_FLEET_4XLARGE: str = field(default="rd-fleet.4xlarge")
    RD_FLEET_2XLARGE: str = field(default="rd-fleet.2xlarge")
    RD_FLEET_XLARGE: str = field(default="rd-fleet.xlarge")
    C_FLEET_2XLARGE: str = field(default="c-fleet.2xlarge")  # For single-threaded jobs
    C_FLEET_4XLARGE: str = field(default="c-fleet.4xlarge")  # For single-threaded jobs


US_SER_LICENCES_CTE = """
-- This helps us understand who is a paying SER customer
SELECT DISTINCT o.org_id
FROM edw.silver.fct_license_orders lo
JOIN datamodel_core.dim_organizations o
    ON lo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
WHERE
    product_sku = 'LIC-SVC-CM-Review-ENT'
    AND is_trial = FALSE
    AND (DATE(contract_end_date) >= DATE_SUB('{FIRST_PARTITION_START}', 30) OR contract_end_date IS NULL)
    AND (DATE(contract_start_date) <= DATE('{FIRST_PARTITION_START}'))
    AND o.date = '{FIRST_PARTITION_START}'
"""

EU_SER_LICENCES_CTE = """
-- This helps us understand who is a paying SER customer
SELECT DISTINCT o.org_id
FROM edw.silver.fct_license_orders lo
JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    ON lo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
WHERE
    product_sku = 'LIC-SVC-CM-Review-ENT'
    AND is_trial = FALSE
    AND (DATE(contract_end_date) >= DATE_SUB('{FIRST_PARTITION_START}', 30) OR contract_end_date IS NULL)
    AND (DATE(contract_start_date) <= DATE('{FIRST_PARTITION_START}'))
    AND o.date = '{FIRST_PARTITION_START}'
"""

NON_US_REGIONAL_SER_LICENCES_CTE = """
-- This helps us understand who is a paying SER customer
-- This is for EU/CA runs
SELECT DISTINCT o.org_id
FROM edw_delta_share.silver.fct_license_orders lo
JOIN datamodel_core.dim_organizations o
    ON lo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
WHERE
    product_sku = 'LIC-SVC-CM-Review-ENT'
    AND is_trial = FALSE
    AND (DATE(contract_end_date) >= DATE_SUB('{FIRST_PARTITION_START}', 30) OR contract_end_date IS NULL)
    AND (DATE(contract_start_date) <= DATE('{FIRST_PARTITION_START}'))
    AND o.date = '{FIRST_PARTITION_START}'
"""

CA_SER_LICENCES_CTE = """
-- This helps us understand who is a paying SER customer
SELECT DISTINCT o.org_id
FROM edw.silver.fct_license_orders lo
JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o
    ON lo.sam_number = COALESCE(o.salesforce_sam_number, o.sam_number)
WHERE
    product_sku = 'LIC-SVC-CM-Review-ENT'
    AND is_trial = FALSE
    AND (DATE(contract_end_date) >= DATE_SUB('{FIRST_PARTITION_START}', 30) OR contract_end_date IS NULL)
    AND (DATE(contract_start_date) <= DATE('{FIRST_PARTITION_START}'))
    AND o.date = '{FIRST_PARTITION_START}'
"""


@dataclass
class ColumnDescription:
    DATE: str = field(default="The date this row/event occurred on")
    ORG_ID: str = field(
        default="The Samsara cloud dashboard ID that the data belongs to"
    )
    DEVICE_ID: str = field(
        default="The ID of the customer device that the data belongs to"
    )
    GATEWAY_ID: str = field(
        default="The ID of the Samsara gateway that the data belongs to"
    )
    GROUP_ID: str = field(default="The ID of the group that the data belongs to")
    DRIVER_ID: str = field(default="The ID of the driver that the data belongs to")
    SAM_NUMBER: str = field(
        default="Samnumber is a unique id that ties customer accounts together in Netsuite, Salesforce, and Samsara cloud dashboard."
    )
    MARKET: str = field(default="The market that the data belongs to")
    QUARANTINE_ENABLED: str = field(
        default="Denotes if quarantine is enabled on the organization. Quarantine implies customers may imply we are no long transmitting logs until a customer left quarantine. This happens what a customer fails to make payments."
    )
    INTERNAL_TYPE_NAME: str = field(
        default="The type of organization a deivce is in (ie customer, internal, test, etc)."
    )
    BUILD: str = field(default="Identifier of the firmware build present on a deivce.")
    PRODUCT_GROUP_ID: str = field(
        default="A Rollout Stage Product Group is a list of Product IDs which share default-program org-based rollout stage enrollment. For example, VGs and CMs all share the same SafetyFleet product group as defined here."
    )
    PRODUCT_PROGRAM_ID: str = field(
        default="A Release Program is the ability to ship a firmware at scale to a set of gateways and orgs. This can be in the form of the Default Program (GA release, program ID 0) or a Non-Default Program (a.k.a. targeted-program, or custom release, program ID != 0)."
    )
    FIRMWARE_PROGRAM_ENROLLMENT_TYPE: str = field(
        default="The type of Release Program enrollment can be gateway-level or org-level, or 'override' if there's a gateway-level or org-level firmware override. As such, possible values are: 'override', 'gateway', or 'org'."
    )
    ROLL_OUT_STAGE_ID: str = field(
        default="A Rollout Stage is a group of organizations (for org-level enrollment) or a group of gateways (for gateway-level enrollment) that receive the same firmware at the same time in the firmware release process. They are the equivalent of “cells” for Samsara’s Cloud Engineering teams.",
    )
    PRODUCT_NAME: str = field(
        default=(
            "A product change requiring multiple versions of the same SKU being manufactured in parallel."
            "Example: Building with two different MCUs for a given SKU, you can track using “HW-SKU-NA A0-000” and “HW-SKU-NA A1-000."
            "NOTE: these changes must not require visibility at the sales/customer/logistics level."
        )
    )
    VARIANT_NAME: str = field(
        default=(
            "A product change requiring multiple versions of the same SKU being manufactured in parallel."
            "Example: Building with two different MCUs for a given SKU, you can track using “HW-SKU-NA A0-000” and “HW-SKU-NA A1-000”."
            "NOTE: these changes must not require visibility at the sales/customer/logistics level."
        )
    )
    CABLE_NAME: str = field(
        default="The name of the cable that the data belongs to. Example: 9-pin, 16-pin, etc."
    )
    MAKE: str = field(
        default="The make of the vehicle that the data belongs to. Example: Ford, Chevrolet, etc."
    )
    MODEL: str = field(
        default="The model of the vehicle that the data belongs to. Example: F-150, Silverado, etc."
    )
    YEAR: str = field(default="The year of the vehicle (ie 2021)")
    ENGINE_MODEL: str = field(default="The engine model of the vehicle.")
    FUEL_TYPE: str = field(default="The Canonical Fuel Type of a vehicle.")
    ENGINE_TYPE: str = field(default=("The Canonical Engine Type of a vehicle."))
    SIGNAL: str = field(default="The signal that the data belongs to")
    BUS: str = field(default="The bus that the data belongs to")
    SOURCE: str = field(default="The source that the data belongs to")
    PRIMARY_FUEL_TYPE: str = field(
        default="The primary fuel type of the vehicle. This field is sourced from a VIN decorder."
    )
    SECONDARY_FUEL_TYPE: str = field(
        default="The secondary fuel type of the vehicle. This field is sourced from a VIN decorder."
    )
    ELECTRIFICATION_LEVEL: str = field(
        default=(
            "The electrification level of the vehicle. Electrication is a ranking between ICE, Hybrid, Plug-In Hybrid, and Full Electric."
            "This field is sourced from a VIN decorder."
        )
    )
    POPULATION_COUNT: str = field(
        default="The number of devices that the data belongs to"
    )
    TYPE: str = field(default="The table from which this fact was extracted from.")
    BUS_ID: str = field(default="The network identifier of a diagnostic command.")
    REQUEST_ID: str = field(default="The request identifier of a diagnostic command.")
    RESPONSE_ID: str = field(default="The response identifier of a command request.")
    OBD_VALUE: str = field(default="The OBD_VALUE reported by a given objectstat.")
    SUM: str = field(default="Sum of valid data points.")
    COUNT: str = field(default="Number of valid data points.")
    AVG: str = field(default="Average aggregated over a day for a value.")
    STDDEV: str = field(default="Standard deviation aggregated over a day for a value.")
    VARIANCE: str = field(default="Variance aggregated over a day for a value.")
    MEAN: str = field(default="Mean aggregated over a day for a value.")
    MEDIAN: str = field(default="Median aggregated over a day for a value.")
    MODE: str = field(default="Mode aggregated over a day for a value.")
    FIRST: str = field(default="First value seen in a day.")
    LAST: str = field(default="Last value seen in a day.")
    KURTOSIS: str = field(
        default="Kurtosis (skewness) aggregated over a day for a value."
    )
    PERCENTILE: str = field(default="Percentile graph for device.")
    HISTOGRAM: str = field(default="Histogram for device.")
    MIN: str = field(default="Minimum seen in a day.")
    MAX: str = field(default="Maximum seen in a day.")
    COUNT_BOUND_ANOMALY: str = field(
        default=(
            "Number of data points seen in a day which are outside bounds defined in definitions.object_stat_field_limits."
        )
    )
    COUNT_DELTA_ANOMALY: str = field(
        default=(
            "Number of data points seen where change from the previous point is beyond limits defined in definitions.object_stat_field_limits."
        )
    )
    COUNT_DELTA_OVER_TIME_ANOMALY: str = field(
        default=(
            "Number of data points seen where velocity from the previous point is beyond limits defined in definitions.object_stat_field_limits."
        )
    )
    METRIC_NAME: str = field(default="A representation of the metric being aggregated.")
    POPULATION_ID: str = field(default="A unique ID for a particular device population")
    POPULATION_NAME: str = field(default="The name of a particular device population")
    TIME: str = field(default="Unix timestamp (ms) of event")
    END_TIME: str = field(default="Unix timestamp (ms) of the end of the event")
    VALUE: str = field(
        default="This is the value of the signal referenced by the combination of type and value; e.g., if type = kinesisstats.osdenginegauge and field = battery_milli_v, then this field captures the voltage of the device battery at a given time."
    )


class DQCheckMode(Enum):
    """Details the way DQ checks should be run on the result of a query."""

    RUN_PER_PARTITION = "per_partition"
    """Run DQ checks individually on every partition populated by the run."""

    WHOLE_RESULT = "whole_result"
    """Run DQ checks on the entire result of the query."""


class ColumnType(Enum):
    """Enum for different types of columns in SQL statements."""

    DIMENSION = "dimension"
    METRIC = "metric"


class DimensionDataType(Enum):
    STRING = "string"
    BOOLEAN = "boolean"
    BIGINT = "bigint"


@dataclass
class DimensionConfig:
    """Configuration for a dimension column."""

    name: str
    data_type: DimensionDataType
    default_value: str
    output_name: Optional[str] = None


REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES = [
    AssetKey(
        [
            AWSRegion.US_WEST_2,
            Database.PRODUCT_ANALYTICS_STAGING,
            "stg_organization_categories",
        ]
    ),
    AssetKey(
        [
            AWSRegion.EU_WEST_1,
            Database.PRODUCT_ANALYTICS_STAGING,
            "stg_organization_categories_eu",
        ]
    ),
    AssetKey(
        [
            AWSRegion.CA_CENTRAL_1,
            Database.PRODUCT_ANALYTICS_STAGING,
            "stg_organization_categories_ca",
        ]
    ),
]
