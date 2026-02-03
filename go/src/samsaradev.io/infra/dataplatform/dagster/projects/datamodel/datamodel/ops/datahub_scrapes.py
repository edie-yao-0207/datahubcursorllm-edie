import datetime
from typing import List

import boto3
import pandas as pd
from dagster import (
    AssetKey,
    AssetsDefinition,
    BackfillPolicy,
    DailyPartitionsDefinition,
    asset,
)
from pyspark.sql import DataFrame, SparkSession

from ..common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ..common.utils import (
    AWSRegions,
    DQGroup,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    apply_db_overrides,
    build_table_description,
    get_code_location,
    get_datahub_env,
    slack_custom_alert,
)
from ..ops.datahub.certified_tables import beta_tables

if get_datahub_env() == "prod":
    DATABASE = "auditlog"
else:
    DATABASE = "dataplatform_dev"

daily_partition_def = DailyPartitionsDefinition(start_date="2024-03-10", end_offset=0)

databases = {
    "database_silver": "auditlog",
}

database_dev_overrides = {
    "database_silver_dev": "datamodel_dev",
}

databases = apply_db_overrides(databases, database_dev_overrides)


@asset(
    required_resource_keys={
        "databricks_pyspark_step_launcher_datahub_scrapes_high_memory"
    },
    partitions_def=daily_partition_def,
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents a daily scrape of DataHub table assets.
            It is meant to capture the current state of all metadata for tables in DataHub.
        """,
        row_meaning="""One row in this table represents a DataHub dataset (table, metric or view), as observed on a given date.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    deps={"scrape_datahub": AssetKey("scrape_datahub")},
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver_dev"]],
    group_name="datahub",
    metadata={
        "code_location": get_code_location(),
        "owners": ["team:DataEngineering"],
        "description": build_table_description(
            table_desc="""This table presents a daily scrape of DataHub table assets.
            It is meant to capture the current state of all metadata for tables in DataHub.
        """,
            row_meaning="""One row in this table represents a DataHub dataset (table, metric or view), as observed on a given date.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
        "schema": [
            {
                "name": "date",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Date of table snapshot"},
            },
            {
                "name": "urn",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "DataHub unique resource key"},
            },
            {
                "name": "database",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Database name of asset in DataHub"},
            },
            {
                "name": "table",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Table name of asset in DataHub"},
            },
            {
                "name": "poll_date_hour",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "Hour that the data was polled"},
            },
            {
                "name": "has_schema",
                "type": "boolean",
                "nullable": True,
                "metadata": {
                    "comment": "Whether the asset has a schema defined in DataHub or not"
                },
            },
            {
                "name": "storage_location",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "s3 location or table (for warehouse view) backing the asset"
                },
            },
            {
                "name": "domain",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "DataHub organizational domain of asset"},
            },
            {
                "name": "last_update",
                "type": "double",
                "nullable": True,
                "metadata": {
                    "comment": "Last update timestamp recorded from hourly metadata job"
                },
            },
            {
                "name": "table_type",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Table type of asset in DataHub - either table or view"
                },
            },
            {
                "name": "num_assertions",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "comment": "Number of data quality checks (assertions) defined for the asset"
                },
            },
            {
                "name": "num_queries",
                "type": "double",
                "nullable": True,
                "metadata": {
                    "comment": "Number of queries run in the last month by Databricks users"
                },
            },
            {
                "name": "num_users",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "comment": "Number of unique Databricks users who have run queries on the asset in the last month"
                },
            },
            {
                "name": "table_description",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Description of the table"},
            },
            {
                "name": "schema",
                "type": "double",
                "nullable": True,
                "metadata": {"comment": "DEPRECATED - DO NOT USE"},
            },
            {
                "name": "valid_glue_schema",
                "type": "boolean",
                "nullable": True,
                "metadata": {
                    "comment": "Whether the schema defined in Glue is valid or not. An invalid Glue schema has been corrupted for various reasons and must be updated by scraping the schema out of Delta files"
                },
            },
            {
                "name": "datahub_schema",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {
                        "type": "map",
                        "keyType": "string",
                        "valueType": "string",
                        "valueContainsNull": True,
                    },
                    "valueContainsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "Schema scraped from DataHub after ingesting Glue and Delta files"
                },
            },
            {
                "name": "sql_sources_schema",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": "string",
                    "valueContainsNull": True,
                },
                "nullable": True,
                "metadata": {
                    "comment": "[DEPRECATED] Schema scraped from Glue, prior to DataHub schema ingestion"
                },
            },
            {
                "name": "github_url",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "GitHub URL where the asset is defined / created"
                },
            },
            {
                "name": "fields_count",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "Number of fields in the schema for the asset"},
            },
            {
                "name": "fields_with_column_descriptions_count",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "Number of fields with column descriptions"},
            },
            {
                "name": "fields_with_quality_column_descriptions_count",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "comment": "Number of fields with high quality column descriptions"
                },
            },
            {
                "name": "data_contract_url",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "URL to data contract for the asset"},
            },
            {
                "name": "owner",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Team that owns the asset"},
            },
            {
                "name": "highlighted_query_count",
                "type": "long",
                "nullable": True,
                "metadata": {
                    "comment": "Number of curated/ highlighted queries for the asset"
                },
            },
            {
                "name": "certified",
                "type": "boolean",
                "nullable": True,
                "metadata": {"comment": "Whether the asset is certified or not"},
            },
            {
                "name": "beta",
                "type": "boolean",
                "nullable": True,
                "metadata": {"comment": "Whether the asset is a beta table or not"},
            },
            {
                "name": "incident_status",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Incident status of the asset"},
            },
            {
                "name": "assertion_status",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Assertion status of the asset"},
            },
            {
                "name": "tags",
                "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": True,
                },
                "nullable": True,
                "metadata": {"comment": "Tags associated with the asset"},
            },
        ],
    },
)
def datahub_scrapes(context) -> DataFrame:
    s3 = boto3.client("s3")

    bucket = "samsara-amundsen-metadata"

    date = str(context.asset_partition_key_for_output())

    context.log.info(date)

    key = f"staging/scrapes/{get_datahub_env()}/datahub_{date}.json"

    obj = s3.get_object(Bucket=bucket, Key=key)

    df = pd.read_json(obj["Body"])

    df["date"] = date

    df["storage_location"] = df["storage_location"].fillna("no_s3_location")

    df["domain"] = df["domain"].fillna("no_domain")

    df["table_type"] = df["table_type"].fillna("no_table_type")

    df["table_description"] = df["table_description"].fillna("no_table_description")

    df["num_queries"] = df["num_queries"].fillna(0)

    # cast num_users to int
    df["num_users"] = df["num_users"].fillna(0).astype(int)

    cols = ["date"] + [col for col in df if col != "date"]
    df = df[cols]

    df = df.drop(columns=["glossary_terms"])
    df["sql_sources_schema"] = pd.Series([{"": ""} for _ in range(len(df))])

    context.log.info(df.head(1).T)

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    pyspark_df = spark.createDataFrame(df)

    context.log.info(pyspark_df.schema.jsonValue()["fields"])

    return pyspark_df


dqs = DQGroup(
    group_name="datahub",
    partition_def=daily_partition_def,
    slack_alerts_channel="alerts-data-tools",
)


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher_datahub_scrapes"},
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="This table contains information about beta tables from certified_tables.",
        row_meaning="Each row represents a beta table and its associated metadata.",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_hours=24,
    ),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver_dev"]],
    partitions_def=daily_partition_def,
    group_name="datahub",
    metadata={
        "code_location": get_code_location(),
        "owners": ["team:DataEngineering"],
        "description": build_table_description(
            table_desc="This table contains information about beta tables from certified_tables.",
            row_meaning="Each row represents a beta table and its associated metadata.",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_hours=24,
        ),
        "schema": [
            {"name": "table_name", "type": "string", "nullable": True, "metadata": {}},
            {
                "name": "expiration_date",
                "type": "string",
                "nullable": True,
                "metadata": {},
            },
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        ],
    },
)
def datahub_beta_tables(context, datahub_scrapes: DataFrame) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    one_week_from_now = datetime.datetime.now() + datetime.timedelta(days=7)
    for table_name, expiration_date in beta_tables.items():
        expiration_datetime = datetime.datetime.strptime(expiration_date, "%Y-%m-%d")
        if expiration_datetime <= one_week_from_now:
            context.log.warn(
                f"Beta table {table_name} is expiring soon on {expiration_date}. You must certify it before then or Dagster unit tests will fail"
            )
            slack_custom_alert(
                f"#{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}",
                f"Beta table {table_name} is expiring soon on {expiration_date}. You must certify it before then or Dagster unit tests will fail",
            )

    df = pd.DataFrame(beta_tables.items(), columns=["table_name", "expiration_date"])

    if df.empty:
        df = pd.DataFrame(
            [["NO_BETA_TABLES_DUMMY_RECORD", "2030-12-31"]],
            columns=["table_name", "expiration_date"],
        )

    df["date"] = str(context.asset_partition_key_for_output())

    pyspark_df = spark.createDataFrame(df)

    return pyspark_df


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher_datahub_scrapes"},
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="This table contains information about DataHub events.",
        row_meaning="Each row represents a DataHub event and its associated metadata.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_hours=24,
    ),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver_dev"]],
    partitions_def=daily_partition_def,
    group_name="datahub",
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=7),
    metadata={
        "code_location": get_code_location(),
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "mp_insert_id"],
        "description": build_table_description(
            table_desc="This table contains information about DataHub events.",
            row_meaning="Each row represents a DataHub event and its associated metadata.",
            table_type=TableType.TRANSACTIONAL_FACT,
            freshness_slo_hours=24,
        ),
        "schema": [
            {
                "name": "date",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Date of the event"},
            },
            {
                "name": "server_timestamp",
                "type": "double",
                "nullable": True,
                "metadata": {"comment": "Server timestamp of the event"},
            },
            {
                "name": "client_timestamp",
                "type": "double",
                "nullable": True,
                "metadata": {"comment": "Client timestamp of the event"},
            },
            {
                "name": "mp_insert_id",
                "type": "string",
                "nullable": True,
                "metadata": {
                    "comment": "Insert ID of the event (generated by Mixpanel)"
                },
            },
            {
                "name": "user_id",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "User ID of the event"},
            },
            {
                "name": "current_url",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Current URL of the event"},
            },
            {
                "name": "event_type",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Type of the event"},
            },
            {
                "name": "entity_urn",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "DataHub urn of the entity"},
            },
            {
                "name": "entity_type",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "DataHub type of the entity"},
            },
            {
                "name": "custom_event_data",
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "search_data",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "query_str",
                                        "type": "string",
                                        "nullable": True,
                                        "metadata": {
                                            "comment": "Query string of the event"
                                        },
                                    },
                                    {
                                        "name": "result_count",
                                        "type": "double",
                                        "nullable": True,
                                        "metadata": {
                                            "comment": "Number of search results returned by the query"
                                        },
                                    },
                                    {
                                        "name": "result_click_index",
                                        "type": "double",
                                        "nullable": True,
                                        "metadata": {
                                            "comment": "Index of the clicked search result"
                                        },
                                    },
                                    {
                                        "name": "result_load_time",
                                        "type": "double",
                                        "nullable": True,
                                        "metadata": {
                                            "comment": "Time taken to load search results in milliseconds (as measured by browser client)"
                                        },
                                    },
                                ],
                            },
                            "nullable": False,
                            "metadata": {"comment": "Search data for the event"},
                        }
                    ],
                },
                "nullable": True,
                "metadata": {"comment": "Custom event data for the event"},
            },
            {
                "name": "user_profile",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "Profile of the user"},
            },
        ],
    },
)
def fct_datahub_events(context, datahub_beta_tables) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # partition_date_str = context.partition_key

    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end

    mixpanel_event_tables = [
        "browsev2togglenodeevent",
        "createviewevent",
        "entitysectionviewevent",
        "entityviewevent",
        "homepagebrowseresultclickevent",
        "homepagesearchevent",
        "homepageviewevent",
        "recommendationclickevent",
        "recommendationimpressionevent",
        "searchacrosslineageresultsviewevent",
        "searchevent",
        "selectautocompleteoption",
        "updateviewevent",
        "visuallineageexpandgraphevent",
        "visuallineageviewevent",
        "searchresultstryagainclickevent",
    ]

    events_with_extra_columns = {
        "searchresultsviewevent": """
            STRUCT(
                STRUCT(
                query AS `query_str`,
                total AS `result_count`,
                null AS `result_click_index`,
                searchresultloadduration AS `result_load_time`
                ) as `search_data`
            )
        """,
        "searchresultclickevent": """
            STRUCT(
                STRUCT(
                query AS `query_str`,
                total AS `result_count`,
                index as `result_click_index`,
                null AS `result_load_time`
                ) as `search_data`
            )
        """,
    }

    query_fragments = []

    all_events = {k: "null" for k in mixpanel_event_tables}

    all_events.update(events_with_extra_columns)

    assert len(mixpanel_event_tables) + len(events_with_extra_columns) == len(
        all_events
    ), "events_with_extra_columns and mixpanel_event_tables must be mutually exclusive"

    table_columns = {}
    for table in mixpanel_event_tables:
        df = spark.sql(f"DESCRIBE mixpanel_samsara.{table}")
        columns = [
            row.col_name for row in df.collect() if not row.col_name.startswith("#")
        ]
        table_columns[table] = columns

    for table, struct_def in all_events.items():

        columns = table_columns.get(table, [])

        query_fragments.append(
            f"""
            SELECT
                mp_date as date,
                MIN(mp_mp_api_timestamp_ms) as server_timestamp,
                MIN(res_timestamp) as client_timestamp,
                mp_insert_id,
                ANY_VALUE(REPLACE(COALESCE(actorurn, concat('urn:li:corpuser:', mp_user_id)), 'urn:li:corpuser:', '')) as user_id,
                ANY_VALUE(mp_current_url) as current_url,
                type as event_type,
                MAX({'entityurn' if 'entityurn' in columns else 'null'}) as entity_urn,
                MAX({'entitytype' if 'entitytype' in columns else 'null'}) as entity_type,
                ANY_VALUE({struct_def}) as custom_event_data
            from
                mixpanel_samsara.{table}
            WHERE mp_date between '{start_date}' and '{end_date}'
            AND mp_current_url not like '%localhost:9002%'
            GROUP BY mp_date,mp_insert_id,type
        """
        )

    query = "UNION ALL\n".join(query_fragments)

    final_query = f"""
        WITH datahub_usage_events AS ({query}),
        deduped_employees AS (
            SELECT *
            FROM edw.silver.employee_hierarchy_hst
            WHERE employee_id IS NOT NULL
            AND is_active = TRUE
            AND is_worker_active = TRUE
        )

        SELECT datahub_usage_events.*,
        CASE
        WHEN CONCAT(manager_data.first_name, ' ', manager_data.last_name) IN ('Katherine Livins', 'Jonathan Cobian', 'Sowmya Murali')
             OR CONCAT(employee_data.first_name, ' ', employee_data.last_name) = 'Katherine Livins'
        THEN 'R&D Data Team'
        WHEN employee_data.department = '100000 - Engineering'
            AND employee_data.job_family IN ('Product Management') THEN 'R&D Product Manager'
        WHEN employee_data.department = '100000 - Engineering'
            AND employee_data.job_family IN
                ('Applied Science',
                'Machine Learning',
                'Hardware Engineering',
                'Software Engineering',
                'Firmware',
                'QA Engineering')
            THEN 'R&D Engineer'
        WHEN employee_data.department = '100000 - Engineering' THEN 'R&D Other'
        ELSE 'Non R&D'
        END AS user_profile

        FROM datahub_usage_events
        LEFT OUTER JOIN deduped_employees employee_data
        ON employee_data.employee_email = replace(user_id, 'urn:li:corpuser:', '')
        LEFT OUTER JOIN deduped_employees manager_data
        ON employee_data.mgr_employee_id = manager_data.employee_id
    """

    context.log.info(final_query)

    df = spark.sql(final_query)

    return df


dqs["fct_datahub_events"].append(
    NonEmptyDQCheck(
        name="check_fct_datahub_events_for_empty_rows",
        database=databases["database_silver_dev"],
        table="fct_datahub_events",
        blocking=False,
    )
)

dqs["fct_datahub_events"].append(
    PrimaryKeyDQCheck(
        name="check_fct_datahub_events_for_primary_key_uniqueness",
        database=databases["database_silver_dev"],
        table="fct_datahub_events",
        primary_keys=["date", "mp_insert_id"],
        blocking=True,
    )
)

dq_assets: List[AssetsDefinition] = dqs.generate()
