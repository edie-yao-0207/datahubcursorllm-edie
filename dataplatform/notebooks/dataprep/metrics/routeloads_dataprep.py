# Databricks notebook source
# MAGIC %run /backend/backend/bigquery/bigquery_pipeline

# COMMAND ----------

import string
from pyspark.sql.functions import *
from delta.tables import *

# formatting rules for identifying IDs in URL
formatting_rules = [
    lambda s: any(x.isupper() for x in s) and any(x.islower() for x in s),
    lambda s: any(x.isdigit() for x in s)
    and any(x in string.punctuation and x != "_" for x in s),
    lambda s: any(x.isdigit() for x in s) and any(x.isupper() for x in s),  # DB* ids
    lambda s: any(x.isdigit() for x in s) and len(s) >= 25,  # longer ids
    lambda s: any(x == "@" for x in s),  # emails
]


def format_url(url):
    # remove query/url params
    queryless_url = url.split("?")[0]

    # remove base url, starting from 3rd element to account for https://
    route_path = queryless_url.split("/")[3:]

    # capture route, replacing ids with common char for aggregation
    for i in range(len(route_path)):
        if route_path[i].isdigit() or any(
            rule(route_path[i]) for rule in formatting_rules
        ):
            route_path[i] = "*"

    # additional trimming, removing o/*/ and groups/*/ and empty elements to more aggressively aggregate
    if len(route_path) > 2 and route_path[0] == "o":
        route_path = route_path[2:]
    if len(route_path) > 2 and (route_path[0] == "groups" or route_path[0] == "g"):
        # assuming groups and g both stand for the same thing in url
        route_path = route_path[2:]
    while len(route_path) > 1 and (
        route_path[-1] == "*" or route_path[-1] == "" or route_path[-1] == "#"
    ):
        route_path = route_path[:-1]

    # join and normalize, remove any last punctuation
    formatted_path = "/".join(route_path).lower()
    while len(formatted_path) > 0 and formatted_path[-1] in string.punctuation:
        formatted_path = formatted_path[:-1]
    return formatted_path


spark.udf.register("format_url", format_url)

# COMMAND ----------

bigquery_pipeline("backend.routeload_clean", "Timestamp", "routeload_logs", "routeload")

# COMMAND ----------

routeload_data = spark.sql(
    """
                           select distinct
                              ServiceName as service_name,
                              ResourceName as resource_name,
                              Url as url,
                              format_url(Url) as formatted_url,
                              Cell as cell,
                              OrgId as org_id,
                              orgs.name as org_name,
                              UserId as user_id,
                              UserEmail as user_email,
                              DurationMs as duration_ms,
                              Timestamp as timestamp,
                              date(Timestamp) as date,
                              unix_timestamp(Timestamp) * 1000 as timestamp_ms,
                              Platform as platform,
                              OS as os,
                              BrowserName as browser_name,
                              BrowserVersion as browser_version,
                              Status as status,
                              TraceId as trace_id,
                              InitialLoad as initial_load,
                              RouteOwner as route_owner,
                              SloGrouping as slo_grouping,
                              GraphqlErrorCode as graphql_error_code,
                              ShowIASidebarNav as showiasidebarnav
                           from routeload_logs.routeload
                           join clouddb.organizations orgs on orgs.id == OrgId
                           """
)

routeload_data.write.format("delta").mode("ignore").partitionBy("date").saveAsTable(
    "dataprep.routeload_data"
)
routeload_data_updates = routeload_data.filter(
    col("date") >= date_sub(current_date(), 3)
)
routeload_data_updates.write.format("delta").mode("overwrite").partitionBy(
    "date"
).option("replaceWhere", "date >= date_sub(current_date(), 3)").saveAsTable(
    "dataprep.routeload_data"
)
