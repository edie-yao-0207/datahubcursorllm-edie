from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    ValueRangeDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
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
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of organization sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.SAM_NUMBER},
    },
    {
        "name": "tenure_months",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Tenure of the org (in months) sourced from datamodel_core.dim_organizations_tenure."
        },
    },
    {
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid telematics customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid telematics customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "raw_average_daily_mileage_per_device",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Raw value of average daily mileage per VG device over the past 28 days"
        },
    },
    {
        "name": "avg_mileage",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for 'avg_mileage', one of ('less_than_250', 'greater_than_250', 'all')."
        },
    },
    {
        "name": "raw_max_usa_subregion",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Subregion name within the US associated with maximal amount of trips in previous 28 days."
        },
    },
    {
        "name": "raw_max_proportion_duration_usa_subregion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of trips in previous 28 days starting in subregion for largest fraction of trips per organization."
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization into 'region' category, one of ('northeast','midwest','southeast','southwest', 'west', 'usa_all', 'canada', 'mexico','all')."
        },
    },
    {
        "name": "raw_fleet_size",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of active VG devices in previous 28 days for organization."
        },
    },
    {
        "name": "fleet_size",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization into 'fleet_size' category, one of ('less_than_30', 'btw_31_and_100', 'btw_101_and_500', 'greater_than_500', 'all')."
        },
    },
    {
        "name": "industry_vertical_raw",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Raw value of org`s industry vertical sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'industry_vertical' category, one of ('field_services', 'transportation', 'public_sector', 'all')."
        },
    },
    {
        "name": "raw_fuel_category_diesel",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with diesel vehicles sourced from product_analytics_staging.stg_daily_fuel_type."
        },
    },
    {
        "name": "raw_fuel_category_gasoline",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with gasoline powered vehicles sourced from product_analytics_staging.stg_daily_fuel_type"
        },
    },
    {
        "name": "raw_fuel_category_electric",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with electric vehicles sourced from product_analytics_staging.stg_daily_fuel_type."
        },
    },
    {
        "name": "raw_fuel_category_other",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with vehicles not in (diesel, gas, electric) sourced from product_analytics_staging.stg_daily_fuel_type"
        },
    },
    {
        "name": "fuel_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization to 'fuel_category' category, one of ('gas', 'diesel', 'electric', 'mixed', 'all')"
        },
    },
    {
        "name": "raw_primary_driving_environment_proportion_urban",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with high population density places."
        },
    },
    {
        "name": "raw_primary_driving_environment_proportion_rural",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with low population density places."
        },
    },
    {
        "name": "raw_primary_driving_environemnt_proportion_mixed",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with medium population density places."
        },
    },
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization to 'primary_driving_environment' category, one of ('urban', 'rural', 'mixed', 'all')."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_heavy",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is above 26,000 lbs."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_medium",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is above 10,000 lbs and less than 26,000 lbs."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_light",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is less than or equal to 10000 lbs."
        },
    },
    {
        "name": "fleet_composition",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'fleet_composition' category, one of ('heavy', 'medium', 'light', 'all')."
        },
    },
    {
        "name": "raw_non_hazmat_eligible_trip_proportion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of fleet`s trips associated with drivers having DOT numbers not eligible for hazmat transport."
        },
    },
    {
        "name": "raw_hazmat_eligible_trip_proportion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of fleet`s trips associated with drivers having DOT numbers eligible for hazmat transport."
        },
    },
    {
        "name": "transport_licenses_endorsements",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'transport_licenses_endorsements' category, one of ('hazmat_goods_carrier', 'all')."
        },
    },
]

QUERY = """--sql
    WITH
    active_devices_l28 AS (
    SELECT org_id,
        device_id,
        latest_date
    FROM datamodel_core.lifetime_device_activity
    WHERE date = (SELECT MAX(date) FROM datamodel_core.lifetime_device_activity)
    AND latest_date >= date_sub('{PARTITION_START}', 28)),
    dim_devices AS (
    SELECT org_id,
        device_id,
        LOWER(associated_vehicle_make) AS associated_vehicle_make,
        LOWER(associated_vehicle_model) AS associated_vehicle_model,
        associated_vehicle_max_weight_lbs,
        LOWER(associated_vehicle_primary_fuel_type) AS associated_vehicle_primary_fuel_type
    FROM datamodel_core.dim_devices
    WHERE dim_devices.date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
    AND dim_devices.device_type = 'VG - Vehicle Gateway'),
    median_max_weight AS (
    SELECT associated_vehicle_make,
        associated_vehicle_model,
        MEDIAN(associated_vehicle_max_weight_lbs) AS associated_vehicle_max_weight_lbs
    FROM dim_devices
    WHERE associated_vehicle_make IS NOT NULL
        AND  associated_vehicle_model IS NOT NULL
        AND associated_vehicle_max_weight_lbs > 0
    GROUP BY 1,2),
    devices AS (
    SELECT dim_devices.* EXCEPT(associated_vehicle_max_weight_lbs),
        CASE WHEN dim_devices.associated_vehicle_max_weight_lbs > 0 THEN dim_devices.associated_vehicle_max_weight_lbs
                WHEN median_max_weight.associated_vehicle_max_weight_lbs > 0  THEN median_max_weight.associated_vehicle_max_weight_lbs END AS associated_vehicle_max_weight_lbs
    FROM dim_devices
    INNER JOIN active_devices_l28
    ON active_devices_l28.device_id = dim_devices.device_id
    AND active_devices_l28.org_id = dim_devices.org_id
    LEFT OUTER JOIN median_max_weight
    ON COALESCE(dim_devices.associated_vehicle_make, "NONE") = median_max_weight.associated_vehicle_make
    AND COALESCE(dim_devices.associated_vehicle_model, "NONE") = median_max_weight.associated_vehicle_model),
    tenure AS (
    SELECT org_id,
        MAX_BY(tenure, start_date) AS tenure_months
    FROM datamodel_core.dim_organizations_tenure
    WHERE end_date <= '{PARTITION_START}'
    GROUP BY 1),
    primary_driving_environment AS (
    SELECT org_id,
        SUM(CASE WHEN pop_density_mapping = 'rural' THEN total ELSE 0 END) / SUM(total) AS proportion_rural,
        SUM(CASE WHEN pop_density_mapping = 'mixed' THEN total ELSE 0 END) / SUM(total) AS proportion_mixed,
        SUM(CASE WHEN pop_density_mapping = 'urban' THEN total ELSE 0 END) / SUM(total) AS proportion_urban
    FROM  {source}.stg_population_density
    WHERE pop_density_mapping is not null
          AND country IN ('US', 'CA', 'MX')
          AND date >= date_sub('{PARTITION_START}', 28)
    GROUP BY 1),
    fleet_size AS (
    SELECT org_id,
        COUNT(DISTINCT device_id) AS vg_device_count_28d
    FROM devices
    GROUP BY 1),
    orgs AS (
    SELECT dim_organizations.date,
        dim_organizations.org_id,
        dim_organizations.org_name,
        dim_organizations.sam_number,
        dim_organizations.account_industry,
        dim_organizations.account_ai_model_industry_archetype AS ai_model_industry_archetype,
        dim_organizations.account_ai_model_primary_industry AS ai_model_primary_industry,
        dim_organizations.account_ai_model_secondary_industry AS ai_model_secondary_industry,
        dim_organizations.account_id,
        dim_organizations.parent_sam_number,
        dim_organizations.is_paid_telematics_customer,
        dim_organizations.is_paid_safety_customer,
        dim_organizations.locale,
        CASE
        WHEN locale IN ('us', 'pr', 'vi') THEN 'usa' -- USA, Puerto Rico, Virgin Islands
        WHEN locale = 'ca' THEN 'canada'
        END international_region,
        tenure.tenure_months
    FROM datamodel_core.dim_organizations dim_organizations
    LEFT OUTER JOIN tenure ON tenure.org_id = dim_organizations.org_id
    WHERE dim_organizations.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    AND dim_organizations.locale IN ('us', 'pr', 'vi', 'ca', 'mx')
    AND dim_organizations.internal_type = 0 ),
     fmcsa_census AS (
    SELECT  DOT_NUMBER AS dot_number,
            CASE WHEN HM_FLAG = "Y" THEN TRUE ELSE FALSE END AS is_hazmat
    FROM read_files(
        's3://samsara-ca-databricks-workspace/dataengineering/csvs/FMCSA_CENSUS1_2024Oct.txt',
        format => 'csv',
        header => true,
        mode => 'PERMISSIVE')),
    company_census AS (
    SELECT DOT_NUMBER AS dot_number,
          CASE WHEN HM_IND = 'Y' THEN TRUE ELSE FALSE END AS is_hazmat
    FROM read_files(
        's3://samsara-ca-databricks-workspace/dataengineering/csvs/Company_Census_File_20241203.csv',
        format => 'csv',
        header => true,
        mode => 'PERMISSIVE')),
  drivers AS (
    SELECT drivers.org_id,
          drivers.id as driver_id,
          MAX(CASE WHEN fmcsa_census.is_hazmat = TRUE or company_census.is_hazmat = TRUE THEN TRUE ELSE FALSE END) AS is_hazmat
    FROM clouddb.drivers
    LEFT JOIN fmcsa_census ON fmcsa_census.dot_number =  coalesce(case when len(carrier_us_dot_number) > 1 then carrier_us_dot_number else null end, case when len(carrier_us_dot_number_override) > 1 then carrier_us_dot_number_override else null end)
    LEFT JOIN company_census ON company_census.dot_number =  coalesce(case when len(carrier_us_dot_number) > 1 then carrier_us_dot_number else null end, case when len(carrier_us_dot_number_override) > 1 then carrier_us_dot_number_override else null end)
    GROUP BY 1,2),
    trips AS (
    SELECT
        fct_trips.date,
        fct_trips.device_id,
        fct_trips.org_id,
        CASE
            WHEN LOWER(start_state) IN ('me', 'maine', 'nh', 'new hampshire', 'vt', 'vermont', 'ma', 'massachusetts', 'ri', 'rhode island', 'ct', 'connecticut', 'ny', 'new york', 'pa', 'pennsylvania', 'nj', 'new jersey') THEN 'northeast'
            WHEN LOWER(start_state) IN ('oh', 'ohio', 'mi', 'michigan', 'in', 'indiana', 'il', 'illinois', 'wi', 'wisconsin', 'mn', 'minnesota', 'ia', 'iowa', 'mo', 'missouri', 'nd', 'north dakota', 'sd', 'south dakota') THEN 'midwest'
            WHEN LOWER(start_state) IN ('de', 'delaware', 'md', 'maryland', 'va', 'virginia', 'wv', 'west virginia', 'ky', 'kentucky', 'nc', 'north carolina', 'sc', 'south carolina', 'ga', 'georgia', 'fl', 'florida', 'al',
                                        'alabama', 'ms', 'mississippi', 'tn', 'tennessee', 'ar', 'arkansas', 'la', 'louisiana') THEN 'southeast'
            WHEN LOWER(start_state) IN ('tx', 'texas', 'ok', 'oklahoma', 'nm', 'new mexico', 'az', 'arizona', 'co', 'colorado', 'ks', 'kansas', 'ne', 'nebraska') THEN 'southwest'
            WHEN LOWER(start_state) IN ('ca', 'california', 'nv', 'nevada', 'or', 'oregon', 'wa', 'washington', 'id', 'idaho', 'mt', 'montana', 'wy', 'wyoming', 'ut', 'utah', 'hi', 'hawaii', 'ak', 'alaska') THEN 'west'
            ELSE NULL
        END AS usa_subregion,
        fct_trips.duration_mins,
        distance_miles,
        drivers.is_hazmat
    FROM datamodel_telematics.fct_trips fct_trips
    LEFT OUTER JOIN drivers
      ON fct_trips.driver_id = drivers.driver_id
    WHERE fct_trips.date BETWEEN date_add('{PARTITION_START}', -28) AND '{PARTITION_START}'
        AND fct_trips.trip_type = 'location_based'
    ),
    hazmat_eligible_trip_count AS (
    SELECT org_id,
           SUM(CASE WHEN is_hazmat = TRUE THEN 1 ELSE 0 END) / COUNT(*) AS hazmat_eligible_trip_proportion,
           SUM(CASE WHEN is_hazmat = FALSE THEN 1 ELSE 0 END) / COUNT(*) AS non_hazmat_eligible_trip_proportion
    FROM trips
    GROUP BY 1),
    --calculate the average daily mileage per device and take a second average across all devices for an org in a later step
    mileage_per_device AS (
    SELECT org_id,
        device_id,
        COUNT(DISTINCT date) AS dates_with_trips,
        SUM(distance_miles) AS total_miles_driven,
        SUM(distance_miles) / COUNT(DISTINCT date) AS avg_daily_mileage
    FROM trips
    GROUP BY 1,2
    ORDER BY 3 desc),
    avg_mileage AS (
    SELECT org_id,
        SUM(avg_daily_mileage) / COUNT(DISTINCT device_id) AS avg_per_device
    FROM mileage_per_device
    GROUP BY 1),
    total_durations AS (
    SELECT org_id,
        SUM(duration_mins) AS total_duration_mins
    FROM trips
    GROUP BY 1),
    usa_subregion_pre AS (
    SELECT trips.org_id,
        trips.usa_subregion,
        SUM(trips.duration_mins) AS total_duration_mins,
        SUM(trips.duration_mins) / MAX(total_durations.total_duration_mins) AS proportion_duration_usa_subregion
    FROM trips
    LEFT JOIN total_durations
        ON total_durations.org_id = trips.org_id
    WHERE trips.usa_subregion IS NOT NULL
    GROUP BY 1,2),
    usa_subregion AS (
    SELECT org_id,
        MAX_BY(usa_subregion, proportion_duration_usa_subregion) AS max_usa_subregion,
        MAX(proportion_duration_usa_subregion) AS max_proportion_duration_usa_subregion
    FROM usa_subregion_pre
    GROUP BY 1),
    fleet_composition AS (
    SELECT org_id,
        SUM(CASE WHEN associated_vehicle_max_weight_lbs > 0 AND associated_vehicle_max_weight_lbs <= 10000 THEN 1 ELSE 0 END) / COUNT(*) AS proportion_light,
        SUM(CASE WHEN associated_vehicle_max_weight_lbs > 10000 AND associated_vehicle_max_weight_lbs <= 26000 THEN 1 ELSE 0 END) / COUNT(*) AS proportion_medium,
        SUM(CASE WHEN associated_vehicle_max_weight_lbs > 26000 THEN 1 ELSE 0 END) / COUNT(*) AS proportion_heavy
    FROM devices
    GROUP BY 1),
    fuel_category AS (
    SELECT
    stg_daily_fuel_type.org_id,
    SUM(CASE
            WHEN (CASE
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'diesel%' THEN 'diesel'
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'gasoline%' THEN 'gas'
                WHEN stg_daily_fuel_type.fuel_type ILIKE '%chargeable%' THEN 'electric'
                WHEN LEN(stg_daily_fuel_type.fuel_type) > 1 THEN 'other'
            END) = 'diesel' THEN 1 ELSE 0 END)/ COUNT(*) AS proportion_diesel,
    SUM(CASE
            WHEN (CASE
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'diesel%' THEN 'diesel'
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'gasoline%' THEN 'gas'
                WHEN stg_daily_fuel_type.fuel_type ILIKE '%chargeable%' THEN 'electric'
                WHEN LEN(stg_daily_fuel_type.fuel_type) > 1 THEN 'other'
            END) = 'gas' THEN 1 ELSE 0 END)/ COUNT(*) AS proportion_gasoline,
    SUM(CASE
            WHEN (CASE
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'diesel%' THEN 'diesel'
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'gasoline%' THEN 'gas'
                WHEN stg_daily_fuel_type.fuel_type ILIKE '%chargeable%' THEN 'electric'
                WHEN LEN(stg_daily_fuel_type.fuel_type) > 1 THEN 'other'
            END) = 'electric' THEN 1 ELSE 0 END)/ COUNT(*) AS proportion_electric,
    SUM(CASE
            WHEN (CASE
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'diesel%' THEN 'diesel'
                WHEN stg_daily_fuel_type.fuel_type ILIKE 'gasoline%' THEN 'gas'
                WHEN stg_daily_fuel_type.fuel_type ILIKE '%chargeable%' THEN 'electric'
                WHEN LEN(stg_daily_fuel_type.fuel_type) > 1 THEN 'other'
            END) = 'other' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS proportion_other
    FROM product_analytics_staging.stg_daily_fuel_type stg_daily_fuel_type
    INNER JOIN devices
        ON devices.org_id = stg_daily_fuel_type.org_id
        AND devices.device_id = stg_daily_fuel_type.device_id
    WHERE stg_daily_fuel_type.date = (SELECT MAX(DATE) FROM  product_analytics_staging.stg_daily_fuel_type)
    GROUP BY 1)

    SELECT '{PARTITION_START}' AS date,
        orgs.org_id,
        orgs.org_name,
        orgs.sam_number,
        orgs.tenure_months,
        orgs.is_paid_telematics_customer,
        orgs.is_paid_safety_customer,
        avg_mileage.avg_per_device AS raw_average_daily_mileage_per_device,
        COALESCE(
        CASE WHEN avg_mileage.avg_per_device > 0 AND avg_mileage.avg_per_device <= 250 THEN 'less_than_250'
                WHEN avg_mileage.avg_per_device > 250 THEN 'greater_than_250' END, 'all') AS avg_mileage,
        usa_subregion.max_usa_subregion AS raw_max_usa_subregion,
        usa_subregion.max_proportion_duration_usa_subregion AS raw_max_proportion_duration_usa_subregion,
        COALESCE(
        CASE WHEN orgs.international_region IN ('canada', 'mexico') THEN orgs.international_region
                WHEN orgs.international_region = 'usa' and
                (usa_subregion.max_proportion_duration_usa_subregion IS NULL
                    OR usa_subregion.max_proportion_duration_usa_subregion <= .6) THEN 'usa_all'
                WHEN orgs.international_region = 'usa' and
                (usa_subregion.max_proportion_duration_usa_subregion IS NOT NULL
                    OR usa_subregion.max_proportion_duration_usa_subregion > .6) THEN usa_subregion.max_usa_subregion END, 'all') AS region,
        fleet_size.vg_device_count_28d AS raw_fleet_size,
        COALESCE(
        CASE WHEN fleet_size.vg_device_count_28d > 0 AND fleet_size.vg_device_count_28d <= 30 THEN 'less_than_30'
                WHEN fleet_size.vg_device_count_28d > 30 AND fleet_size.vg_device_count_28d <= 100 THEN 'btw_31_and_100'
                WHEN fleet_size.vg_device_count_28d > 100 AND fleet_size.vg_device_count_28d <= 500 THEN 'btw_101_and_500'
                WHEN fleet_size.vg_device_count_28d > 500 THEN 'greater_than_500' END, 'all') AS fleet_size,
        orgs.account_industry AS industry_vertical_raw,
        orgs.ai_model_industry_archetype,
        orgs.ai_model_primary_industry,
        orgs.ai_model_secondary_industry,
        COALESCE(
        CASE WHEN orgs.account_industry IN
                ('Transportation & Warehousing',
                'Wholesale Trade',
                'Passenger Transit',
                'Manufacturing',
                'Food & Beverage',
                'Retail Trade',
                'Consumer Products',
                'Remote Infrastructure') THEN 'transportation'
            WHEN orgs.account_industry IN
                ('Mining, Quarrying, Oil & Gas',
                'Construction',
                'Extraction',
                'Field Services',
                'Utilities') THEN 'field_services'
            WHEN orgs.account_industry IN
                ('Government',
                'Educational Services',
                'Health Care & Social Assistance') THEN 'public_sector' END, 'all') AS industry_vertical,
        fuel_category.proportion_diesel AS raw_fuel_category_diesel,
        fuel_category.proportion_gasoline AS raw_fuel_category_gasoline,
        fuel_category.proportion_electric AS raw_fuel_category_electric,
        fuel_category.proportion_electric AS raw_fuel_category_other,
        COALESCE(
        CASE WHEN fuel_category.proportion_diesel > .5 THEN 'diesel'
                WHEN fuel_category.proportion_electric > .5 THEN 'electric'
                WHEN fuel_category.proportion_gasoline > .5 THEN 'gas'
                WHEN (fuel_category.proportion_diesel + fuel_category.proportion_electric + fuel_category.proportion_gasoline + fuel_category.proportion_other) > .5 THEN 'mixed'
                END, 'all') AS fuel_category,
        primary_driving_environment.proportion_urban AS raw_primary_driving_environment_proportion_urban,
        primary_driving_environment.proportion_rural AS raw_primary_driving_environment_proportion_rural,
        primary_driving_environment.proportion_mixed AS raw_primary_driving_environemnt_proportion_mixed,
        COALESCE(
        CASE WHEN primary_driving_environment.proportion_urban > .5
                AND primary_driving_environment.proportion_rural < .3
                THEN 'urban'
                WHEN primary_driving_environment.proportion_rural > .5
                AND primary_driving_environment.proportion_urban < .3
                THEN 'rural'
                WHEN primary_driving_environment.proportion_rural > 0
                    OR primary_driving_environment.proportion_urban > 0
                    OR primary_driving_environment.proportion_mixed > 0
                    THEN 'mixed' END,'all') AS primary_driving_environment,
        fleet_composition.proportion_heavy AS raw_fleet_composition_proportion_heavy,
        fleet_composition.proportion_medium AS raw_fleet_composition_proportion_medium,
        fleet_composition.proportion_light AS raw_fleet_composition_proportion_light,
        COALESCE(
        CASE WHEN fleet_composition.proportion_heavy > .45 THEN 'heavy'
                WHEN fleet_composition.proportion_light > .45 THEN 'light'
                WHEN fleet_composition.proportion_medium > .45 THEN 'medium'
                WHEN (fleet_composition.proportion_heavy + fleet_composition.proportion_light + fleet_composition.proportion_medium) > .5
                THEN 'mixed' END, 'all') AS fleet_composition,
        hazmat_eligible_trip_count.hazmat_eligible_trip_proportion AS raw_hazmat_eligible_trip_proportion,
        hazmat_eligible_trip_count.non_hazmat_eligible_trip_proportion AS raw_non_hazmat_eligible_trip_proportion,
        CASE WHEN hazmat_eligible_trip_count.hazmat_eligible_trip_proportion > 0 THEN "hazmat_goods_carrier" ELSE 'all' END AS transport_licenses_endorsements
    FROM orgs
    LEFT OUTER JOIN avg_mileage ON avg_mileage.org_id = orgs.org_id
    LEFT OUTER JOIN usa_subregion ON usa_subregion.org_id = orgs.org_id
    LEFT OUTER JOIN fleet_size ON fleet_size.org_id = orgs.org_id
    LEFT OUTER JOIN fuel_category ON fuel_category.org_id = orgs.org_id
    LEFT OUTER JOIN primary_driving_environment ON primary_driving_environment.org_id = orgs.org_id
    LEFT OUTER JOIN fleet_composition ON fleet_composition.org_id = orgs.org_id
    LEFT OUTER JOIN hazmat_eligible_trip_count ON hazmat_eligible_trip_count.org_id = orgs.org_id
--endsql"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Assignment of organizations to categories used for Fleet Benchmarks report for Canada""",
        row_meaning="""Each row represents the calculation of category values per organization_id for one date""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.CA_CENTRAL_1],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_categories_ca"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_categories_ca",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_categories_ca",
            non_null_columns=[
                "date",
                "fleet_composition",
                "primary_driving_environment",
                "fuel_category",
                "industry_vertical",
                "fleet_size",
                "region",
                "avg_mileage",
                "transport_licenses_endorsements",
                "is_paid_telematics_customer",
                "is_paid_safety_customer",
                "org_id",
                "org_name",
            ],
            block_before_write=False,
        ),
        TrendDQCheck(
            name="dq_trend_stg_organization_categories_ca",
            tolerance=0.5,
            lookback_days=1,
            dimension="is_paid_telematics_customer",
            min_percent_share=0.1,
            block_before_write=False,
        ),
        ValueRangeDQCheck(
            name="dq_value_range_stg_organization_categories_ca",
            column_range_map={"raw_fuel_category_diesel": (0, 1)},
            block_before_write=False,
        ),
    ],
    upstreams=[
        "ca-central-1:product_analytics_staging.stg_population_density",
        "ca-central-1:product_analytics_staging.map_population_density",
        "ca-central-1:datamodel_core.dim_organizations",
        "ca-central-1:datamodel_telematics.fct_trips",
        "ca-central-1:datamodel_core.dim_devices",
        "ca-central-1:datamodel_core.lifetime_device_activity",
        "ca-central-1:datamodel_core.dim_organizations_tenure",
    ],
)
def stg_organization_categories_ca(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_organization_categories_ca")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    source = "product_analytics_staging"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    query = QUERY.format(
        source=source,
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
