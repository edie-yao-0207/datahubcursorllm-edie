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

PRIMARY_KEYS = ["join_criteria"]

SCHEMA = [
    {
        "name": "zip",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "For United States locations, zip code is used as the join criteria, alongside country, to map locations to their population density label. For other countries, zip code is null."
        },
    },
    {
        "name": "city",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "For Canada locations, city and state (Canadian province) are used as the join criteria, alongside country, to map locations to their population density label. For United States locations, the name of a city corresponding to a zip code is available but should not be used as join criteria as many cities can correspond to a zip code. City is null for locations in Mexico."
        },
    },
    {
        "name": "state",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "For Canada locations, state (Canadian province) should be used alongside country and city to map locations to their population density. For Mexico locations, state is the sole joining criteria for population density mappings. State is available for United States locations but is not necessary to be used for join criteria as the zip code is sufficient."
        },
    },
    {
        "name": "population",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Population of corresponding zip code for United States locations or city for Canada locations. Used to calculate the population density per square mile if available. Null for Mexico locations."
        },
    },
    {
        "name": "square_mi",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Square miles of corresponding area used to calculate population density. For United States zip codes, the square miles relate to the corresponding county sourced through the zip code's FIPS code from the United States Census. For Canadian locations, the square miles correspond to the city (municipality) and state (province) sourced from Statistics Canada."
        },
    },
    {
        "name": "pop_density_per_mile",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "The quotient of population and square miles for United States zip code's corresponding county or Canadian city (municipality)."
        },
    },
    {
        "name": "country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "One of ('US', 'CA', 'MX'). Should be used as join criteria to locations for all countries."
        },
    },
    {
        "name": "pop_density_mapping",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Mapping of United States zip code or Canadian municipality to population density per square mile."
        },
    },
    {
        "name": "join_criteria",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Synthetic primary key created from (zip, country) for United States,(city, state, country) for Canada, (state, country) for Mexico to demonstrate how to join location metadata sourced from kinesisstats.location. "
        },
    },
]

QUERY = """

with ca_base as (
     SELECT regexp_replace(replace(Municipality, '-', ' '), '[0-9]', '')  AS municipality,
        CASE
            WHEN Province = 'Alta.' THEN 'AB'
            WHEN Province = 'B.C.' THEN 'BC'
            WHEN Province = 'N.S.' THEN 'NS'
            WHEN Province = 'Ont.' THEN 'ON'
            WHEN Province = 'Sask.' THEN 'SK'
            WHEN Province = 'Man.' THEN 'MB'
            WHEN Province = 'Que.' THEN 'QC'
            WHEN Province = 'P.E.I.' THEN 'PE'
            WHEN Province = 'N.W.T.' THEN 'NT'
            WHEN Province = 'Nvt.' THEN 'NU'
            WHEN Province = 'N.L.' THEN 'NL'
            WHEN Province = 'Y.T.' THEN 'YT'
            WHEN Province = 'N.B.' THEN 'NB'
        END AS province,
        COALESCE(TRY_CAST(replace(`Population, 2021`, ',', '') as bigint),0) AS population,
        ROUND(TRY_CAST(replace(`Land area in square kilometres, 2021`, ',', '') as double) * 0.3861,2) AS square_mi,
        coalesce(
            ROUND(try_divide(TRY_CAST(replace(`Population, 2021`, ',', '') as bigint),
            (TRY_CAST(replace(`Land area in square kilometres, 2021`, ',', '') as double)* 0.3861))
                ,2)
                ,0) AS pop_density_per_mile
    FROM read_files(
        's3://{BUCKET_NAME}/dataengineering/csvs/9810000201-eng.csv',
        format => 'csv',
        header => true)
    --https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=9810000201
    ),
    ca as (
    select
        TRANSLATE(
            LOWER(municipality),
            'àáâãäåèéêëìíîïòóôõöùúûüñç',
            'aaaaaaeeeeiiiiooooouuuunc'
        ) AS city,
        province as state,
        sum(population) as population,
        sum(square_mi) as square_mi,
        round(try_divide(sum(population), sum(square_mi)), 2) as pop_density_per_mile
    from ca_base
    group by 1,2),
    mx AS (
    select
        UPPER(state) AS state,
        pop_density_mapping
    FROM VALUES
        ('dif', 'urban'),
        ('nle', 'urban'),
        ('mex', 'urban'),
        ('jal', 'urban'),
        ('roo', 'urban'),
        ('bcn', 'urban'),
        ('chp', 'rural'),
        ('oax', 'rural'),
        ('gro', 'rural'),
        ('mic', 'rural'),
        ('zac', 'rural'),
        ('tla', 'rural'),
        ('dur', 'rural'),
        ('pue', 'mixed'),
        ('gua', 'mixed'),
        ('ver', 'mixed'),
        ('yuc', 'mixed'),
        ('chh', 'mixed'),
        ('son', 'mixed'),
        ('slp', 'mixed'),
        ('que', 'mixed'),
        ('tab', 'mixed'),
        ('hid', 'mixed'),
        ('tam', 'mixed'),
        ('sin', 'mixed'),
        ('agu', 'mixed'),
        ('coa', 'mixed'),
        ('col', 'mixed'),
        ('bcs', 'mixed'),
        ('mor', 'mixed'),
        ('cam', 'mixed'),
        ('nayarit', 'mixed'),
        ('isla mujeres', 'urban')
    AS mapping(state, pop_density_mapping)),
    us_county_metadata AS (
    SELECT ST_ABBR AS state,
    COUNTY AS county_name,
    FIPS AS county_fips,
    AREA_SQMI AS county_area_sqmi,
    E_TOTPOP AS county_total_population
    FROM read_files(
        's3://{BUCKET_NAME}/dataengineering/csvs/SVI_2022_US_county.csv',
        format => 'csv',
        header => true,
        mode => 'FAILFAST')),
    us_zipcode_metadata AS (
    SELECT
    LPAD(CAST(zip AS STRING), 5, '0') AS zip,
    usps_zip_pref_city AS city,
    usps_zip_pref_state AS state,
    LPAD(CAST(county AS STRING), 5, '0')AS county_fips
    FROM read_files(
        's3://{BUCKET_NAME}/dataengineering/csvs/ZIP_COUNTY_062024.csv',
        format => 'csv',
        header => true,
        mode => 'FAILFAST')),
    us_joined AS (
    SELECT zipcode_metadata.zip,
    zipcode_metadata.city,
    zipcode_metadata.state,
    zipcode_metadata.county_fips,
    county_metadata.county_name,
    county_metadata.county_area_sqmi,
    county_metadata.county_total_population,
    round( county_metadata.county_total_population / county_metadata.county_area_sqmi, 2) AS pop_density_per_mile
    FROM us_zipcode_metadata zipcode_metadata
    INNER JOIN us_county_metadata county_metadata
    ON county_metadata.county_fips = zipcode_metadata.county_fips),
    us as (
    SELECT zip,
    UPPER(MAX_BY(state, county_total_population)) AS state,
    LOWER(MAX(city)) AS city,
    MAX_BY(county_fips, county_total_population) AS fips,
    MAX_BY(county_name, county_total_population) AS county_name,
    MAX_BY(county_area_sqmi, county_total_population) AS square_mi,
    MAX(county_total_population) AS population,
    ROUND( MAX(county_total_population) / MAX_BY(county_area_sqmi, county_total_population), 2) AS pop_density_per_mile
    FROM us_joined
    GROUP BY 1)

    (select CAST(NULL AS STRING) AS zip,
        city,
        state,
        population,
        square_mi,
        pop_density_per_mile,
        'CA' AS country,
        case
            when pop_density_per_mile >=  0 AND pop_density_per_mile  < 500 or population < 10000 then 'rural'
            when pop_density_per_mile >=  500 AND pop_density_per_mile  < 1000 then 'mixed'
            when pop_density_per_mile >= 1000 then 'urban' end as pop_density_mapping,
        concat('city:',city,',state:',state, ',country:', 'CA') AS join_criteria
    from ca)
    union all
    (select CAST(NULL AS STRING) AS zip,
            CAST(NULL AS STRING) AS city,
            state,
            CAST(NULL AS BIGINT) AS population,
            CAST(NULL AS DOUBLE) AS square_mi,
            CAST(NULL AS DOUBLE) AS pop_density_per_mile,
            'MX' AS country,
            pop_density_mapping,
            concat('state:',state,',country:', 'MX') as join_criteria
    from mx)
    union all
    (select zip,
            city,
            state,
            population,
            square_mi,
            pop_density_per_mile,
            'US' AS country,
            case
            when pop_density_per_mile >=  0 AND pop_density_per_mile  < 500 or population < 10000 then 'rural'
            when pop_density_per_mile >=  500 AND pop_density_per_mile  < 1000 then 'mixed'
            when pop_density_per_mile >= 1000 then 'urban' end as pop_density_mapping,
        concat('zip:',cast(zip as string),',country:', 'US') as join_criteria
    from us)
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Map of location based identifiers to population density critera to support primary driving environment calculation of rural, urban, or mixed density locations.""",
        row_meaning="""Each row represents metadata for location based entity to be used to map location data- zip code for the United States, city  for Canada, and state for Mexico""",
        related_table_info={},
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2, AWSRegion.CA_CENTRAL_1],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=None,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_map_population_density"),
        PrimaryKeyDQCheck(
            name="dq_pk_map_population_density",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_map_population_density",
            non_null_columns=[
                "state",
                "country",
                "join_criteria",
                "pop_density_mapping",
            ],
            block_before_write=False,
        ),
    ],
)
def map_population_density(context: AssetExecutionContext) -> str:
    context.log.info("Updating map_population_density")
    region = context.asset_key.path[0]
    if region == AWSRegion.US_WEST_2:
        bucket_name = "samsara-databricks-workspace"
    elif region == AWSRegion.CA_CENTRAL_1:
        bucket_name = "samsara-ca-databricks-workspace"
    query = QUERY.format(BUCKET_NAME=bucket_name)
    context.log.info(f"{query}")
    return query
