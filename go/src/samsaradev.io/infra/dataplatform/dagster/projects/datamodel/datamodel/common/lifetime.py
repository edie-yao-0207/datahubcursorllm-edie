from dagster import AutoMaterializePolicy, AutoMaterializeRule

from .utils import AWSRegions, WarehouseWriteMode, build_assets_from_sql


def generate_lifetime_schema():
    schema = [
        {
            "name": "first_date",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": "First occurrence of an action for the unit"},
        },
        {
            "name": "latest_date",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "Latest occurrence of an action for the unit. For VGs, this is defined as when the device was last online (heartbeat) AND had a trip."
            },
        },
        {
            "name": "l1",
            "type": "integer",
            "nullable": False,
            "metadata": {
                "comment": "Over the last 1 day, on how many days did the action occur"
            },
        },
        {
            "name": "l7",
            "type": "integer",
            "nullable": True,
            "metadata": {
                "comment": "Over the last 7 days, on how many days did the action occur"
            },
        },
        {
            "name": "l28",
            "type": "integer",
            "nullable": True,
            "metadata": {
                "comment": "Over the last 28 days, on how many days did the action occur"
            },
        },
        {
            "name": "l_lifetime",
            "type": "integer",
            "nullable": True,
            "metadata": {
                "comment": "Over the unit`s lifetime, on how many days did the action occur"
            },
        },
        {
            "name": "total_1d",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Aggregation of the action over the last day"},
        },
        {
            "name": "total_7d",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Aggregation of the action over the last 7 days"},
        },
        {
            "name": "total_28d",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": "Aggregation of the action over the last 28 days"},
        },
        {
            "name": "date_array",
            "type": {"type": "array", "elementType": "string", "containsNull": True},
            "nullable": True,
            "metadata": {"comment": "Array of the dates when an action occurred"},
        },
        {
            "name": "date_string",
            "type": "string",
            "nullable": True,
            "metadata": {
                "comment": "String of 0s and 1s where every character represents a day in the unit's lifetime. 1 means that the unit was active on that day"
            },
        },
        {
            "name": "metric_array",
            "type": {"type": "array", "elementType": "long", "containsNull": True},
            "nullable": True,
            "metadata": {
                "comment": "Array of metric values. Every value in the array represents a day in the unit`s lifetime"
            },
        },
        {
            "name": "date",
            "type": "string",
            "nullable": False,
            "metadata": {
                "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
            },
        },
    ]
    return schema


class Lifetime:
    def __init__(
        self,
        name,
        description,
        schema,
        primary_keys,
        metric,
        upstreams,
        group_name,
        source_database,
        source_table,
        database,
        databases,
        partitions_def,
        source_filter=None,
        attribute_columns=None,
    ):
        self.name = name
        self.description = description
        self.schema = schema
        self.primary_keys = primary_keys
        self.metric = metric
        self.attribute_columns = attribute_columns
        self.upstreams = upstreams
        self.group_name = group_name
        self.source_database = source_database
        self.source_table = source_table
        self.source_filter = source_filter
        self.database = database
        self.databases = databases
        self.partitions_def = partitions_def

    def generate_first_day_sql(self):
        keys = ",".join(self.primary_keys)

        filter = "1=1" if not self.source_filter else self.source_filter

        attributes_str = (
            ",".join(self.attribute_columns) + "," if self.attribute_columns else ""
        )

        query = """
            SELECT {keys},
                '{DATEID}' AS first_date,
                '{DATEID}' AS latest_date,
                {attributes_str}
                1 AS l1,
                1 AS l7,
                1 AS l28,
                1 AS l_lifetime,
                CAST({metric} AS LONG) AS total_1d,
                CAST({metric} AS LONG) AS total_7d,
                CAST({metric} AS LONG) AS total_28d,
                ARRAY('{DATEID}') AS date_array,
                '1' AS date_string,
                ARRAY(CAST({metric} AS LONG)) AS metric_array,
                '{DATEID}' date
            FROM {source_database}.{source_table}
            WHERE date = '{DATEID}'
            AND {source_filter}
            """.format(
            keys=keys,
            source_database=self.source_database,
            source_table=self.source_table,
            metric=self.metric,
            source_filter=filter,
            attributes_str=attributes_str,
            DATEID="{DATEID}",
        )

        return query

    def generate_daily_sql(self):
        first_key = self.primary_keys[0]

        keys = ",".join(self.primary_keys)

        coalesce = ""
        for col in self.primary_keys:
            coalesce += (
                "COALESCE(lifetime.{col}, daily_snapshot.{col}) AS {col},".format(
                    col=col
                )
            )

        join_cond = ""
        for col in self.primary_keys:
            join_cond += "lifetime.{col} = daily_snapshot.{col} AND ".format(col=col)
        join_cond += "1=1"

        filter = "1=1" if not self.source_filter else self.source_filter

        attributes_str = (
            ",".join(self.attribute_columns) + "," if self.attribute_columns else ""
        )

        attributes_coalesce = ""
        for col in self.attribute_columns:
            coalesce += (
                "COALESCE(daily_snapshot.{col}, lifetime.{col}) AS {col},".format(
                    col=col
                )
            )

        query = """
            --sql
            WITH daily_snapshot
            (
                SELECT {keys},
                    {attributes_str}
                    {metric} AS snapshot_metric
                FROM {source_database}.{source_table}
                WHERE date = '{DATEID}'
                AND {source_filter}
            ),
            lifetime
            (
                SELECT {keys},
                    {attributes_str}
                    date_array,
                    date_string,
                    metric_array,
                    first_date,
                    latest_date
                FROM {lifetime_db}.{lifetime_table}
                WHERE date = DATE_ADD('{DATEID}', -1)
            ),
            full_outer_join
            (
                SELECT {coalesce}
                {attributes_coalesce}
                lifetime.date_array,
                lifetime.date_string,
                lifetime.metric_array,
                lifetime.first_date,
                lifetime.latest_date,
                daily_snapshot.snapshot_metric,
                '{DATEID}' AS snapshot_date,
                CASE
                    WHEN lifetime.{first_key} IS NOT NULL AND daily_snapshot.{first_key} IS NOT NULL THEN 'new row'
                    WHEN lifetime.{first_key} IS NOT NULL AND daily_snapshot.{first_key} IS NULL THEN 'no new rows'
                    WHEN lifetime.{first_key} IS NULL AND daily_snapshot.{first_key} IS NOT NULL THEN 'first row'
                END AS row_type
                FROM lifetime
                FULL OUTER JOIN daily_snapshot
                ON ({join_cond})
            )
            SELECT {keys},
            {attributes_str}
            first_date,
            latest_date,
            CASE
            WHEN row_type IN ('new row', 'first row') THEN 1
            ELSE 0
            END AS l1,

            CASE
                WHEN LENGTH(date_string) <= 7
                THEN LENGTH(REPLACE(date_string, '0', ''))
                ELSE LENGTH(REPLACE(SUBSTR(date_string, LENGTH(date_string) -6, 7), '0', ''))
            END AS l7,

            CASE
                WHEN LENGTH(date_string) <= 28
                THEN LENGTH(REPLACE(date_string, '0', ''))
                ELSE LENGTH(REPLACE(SUBSTR(date_string, LENGTH(date_string) -27, 28), '0', ''))
            END AS l28,
            LENGTH(REPLACE(date_string, '0', '')) AS l_lifetime,
            CAST(ELEMENT_AT(metric_array, SIZE(metric_array)) AS LONG) AS total_1d,
            CAST(AGGREGATE(TRANSFORM(SLICE(metric_array, GREATEST(SIZE(metric_array) - 6, 1), 7),element -> CAST(element AS INT)), 0, (acc, x) -> acc + x) AS LONG) total_7d,
            CAST(AGGREGATE(TRANSFORM(SLICE(metric_array, GREATEST(SIZE(metric_array) - 27, 1), 28),element -> CAST(element AS INT)), 0, (acc, x) -> acc + x) AS LONG) total_28d,
            date_array,
            date_string,
            metric_array,
            '{DATEID}' date
            FROM
            (
            SELECT row_type,
            {attributes_str}
            {keys},
            CASE
                WHEN row_type IN ('new row', 'no new rows') THEN first_date
                ELSE snapshot_date
            END AS first_date,
            CASE
                WHEN row_type IN ('new row', 'first row') THEN snapshot_date
                ELSE latest_date
            END AS latest_date,
            CASE
                WHEN row_type = 'new row' THEN ARRAY_APPEND(date_array, snapshot_date)
                WHEN row_type = 'no new rows' THEN date_array
                WHEN row_type = 'first row' THEN ARRAY(snapshot_date)
            END AS date_array,
            CASE
                WHEN row_type = 'new row' THEN date_string || '1'
                WHEN row_type = 'no new rows' THEN date_string || '0'
                WHEN row_type = 'first row' THEN '1'
            END AS date_string,
            CASE
                WHEN row_type = 'new row' THEN ARRAY_APPEND(metric_array, snapshot_metric)
                WHEN row_type = 'no new rows' THEN ARRAY_APPEND(metric_array, 0)
                WHEN row_type = 'first row' THEN ARRAY(snapshot_metric)
            END AS metric_array
            FROM full_outer_join
            )
            --endsql
        """.format(
            keys=keys,
            first_key=first_key,
            source_database=self.source_database,
            source_table=self.source_table,
            coalesce=coalesce,
            join_cond=join_cond,
            metric=self.metric,
            attributes_str=attributes_str,
            attributes_coalesce=attributes_coalesce,
            lifetime_db=self.database,
            lifetime_table=self.name,
            source_filter=filter,
            DATEID="{DATEID}",
        )

        return query

    def generate_asset(self):
        return build_assets_from_sql(
            name=self.name,
            description=self.description,
            schema=self.schema,
            regions=[
                AWSRegions.US_WEST_2.value,
                AWSRegions.EU_WEST_1.value,
                AWSRegions.CA_CENTRAL_1.value,
            ],
            sql_query=self.generate_daily_sql(),
            primary_keys=self.primary_keys,
            upstreams=self.upstreams,
            group_name=self.group_name,
            database=self.database,
            databases=self.databases,
            write_mode=WarehouseWriteMode.overwrite,
            partitions_def=self.partitions_def,
            query_type="lifetime",
            custom_query_params={"alt_query": self.generate_first_day_sql()},
            depends_on_past=True,
            auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
                AutoMaterializeRule.skip_on_not_all_parents_updated()
            ),
        )
