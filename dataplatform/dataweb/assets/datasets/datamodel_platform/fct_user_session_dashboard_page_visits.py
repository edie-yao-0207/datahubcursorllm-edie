from dagster import AssetExecutionContext
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    Database,
    InstanceType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    TableType,
)


fct_user_session_dashboard_page_visits_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Date when the user's session ended. YYYY-mm-dd."},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Latest organization ID corresponding to the user_id from dim_users_organizations. Joins to datamodel_core.dim_organizations."
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "Unique identifier for a user from dim_users_organizations. Null if the user's email from Mixpanel doesn't match an email in the dim table."},
    },
    {
        "name": "user_email",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Dashboard user's email from dim_users_organizations or Mixpanel."},
    },
    {
        "name": "is_samsara_email",
        "type": "boolean",
        "nullable": True,
        "metadata": {"comment": "Whether the user is from samsara.com."},
    },
    {
        "name": "mixpanel_distinct_id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Mixpanel's unique user identifier, usually an email address or Samsara user_id."},
    },
    {
        "name": "mixpanel_org_id",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "org_id sourced from Mixpanel."},
    },
    {
        "name": "session_id",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier for the user's visit to the cloud dashboard."
        },
    },
    {
        "name": "visit_timestamp",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Timestamp of page visit."},
    },
    {
        "name": "visit_order_number",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Rank indicating the order within the session when the page was visited."
        },
    },
    {
        "name": "visit_duration_seconds",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Duration of page visit, in seconds."},
    },
    {
        "name": "session_start_timestamp",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Earliest timestamp in the session."},
    },
    {
        "name": "session_end_timestamp",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Latest timestamp in the session."},
    },
    {
        "name": "session_duration_seconds",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Total duration of the session, in seconds."},
    },

    {
        "name": "dashboard_url",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "URL of page visited on cloud dashboard."},
    },
    {
        "name": "route_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Canonical name of dashboard page route, from routes.json."
        },
    },
    {
        "name": "route_owner",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Owning team of dashboard page, from routes.json."},
    },
    {
        "name": "route_path",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Normalized route path, from routes.json."},
    },
]

fct_user_session_dashboard_page_visits_query = """
with user_orgs as (
  select
    user_id,
    max_by(org_id, last_web_usage_date) as org_id,
    any_value(email) as email,
    any_value(is_samsara_email) as is_samsara_email
  from
    datamodel_platform.dim_users_organizations
  where
    date = (select max(date) from datamodel_platform.dim_users_organizations)
  group by
    1

  union all

  select
    user_id,
    max_by(org_id, last_web_usage_date) as org_id,
    any_value(email) as email,
    any_value(is_samsara_email) as is_samsara_email
  from
    data_tools_delta_share.datamodel_platform.dim_users_organizations
  where
    date = (select max(date) from data_tools_delta_share.datamodel_platform.dim_users_organizations)
  group by
    1

  union all

  select
    user_id,
    max_by(org_id, last_web_usage_date) as org_id,
    any_value(email) as email,
    any_value(is_samsara_email) as is_samsara_email
  from
    data_tools_delta_share_ca.datamodel_platform.dim_users_organizations
  where
    date = (select max(date) from data_tools_delta_share_ca.datamodel_platform.dim_users_organizations)
  group by
    1
),
page_visits as (
  select
    distinct_id,
    time,
    timestampdiff(
      MINUTE,
      lag(from_unixtime(time)) over (
        partition by distinct_id
        order by
          time asc
      ),
      from_unixtime(time)
    ) as minutes_since_last_page_visit,
    orgid as org_id,
    COALESCE(mp_user_id, mp_device_id) AS mp_user_id,
    mp_current_url,
    mp_duration,
    routename,
    mp_date
  from
    mixpanel_samsara.cloud_route_session
  where
    mp_date between date_add('{PARTITION_START_DATE}', -6) and '{PARTITION_END_DATE}'
),
numbered_sessions as (
  select
    mp_user_id,
    distinct_id,
    org_id,
    from_unixtime(time) as visit_timestamp,
    mp_duration as visit_duration_seconds,
    sum(
      case
        when minutes_since_last_page_visit > 30 then 1
        else 0
      end
    ) over (
      partition by distinct_id
      order by
        time rows between unbounded preceding
        and current row
    ) as session_number,
    mp_current_url as dashboard_url,
    routename as route_name,
    mp_date
  from
    page_visits
),
timestamped_sessions as (
  select
    mp_user_id,
    distinct_id,
    org_id,
    visit_timestamp,
    visit_duration_seconds,
    session_number,
    min(visit_timestamp) over (partition by distinct_id, session_number) as session_start_timestamp,
    max(visit_timestamp) over (partition by distinct_id, session_number) as session_end_timestamp,
    sum(visit_duration_seconds) over (
      partition by distinct_id,
      session_number
    ) as session_duration_seconds,
    rank() over (
      partition by distinct_id,
      session_number
      order by
        visit_timestamp
    ) as visit_order_number,
    dashboard_url,
    route_name,
    mp_date
  from
    numbered_sessions
),
-- routename from Mixpanel isn't a reliable way to join with
-- perf_infra.route_config to get route paths and owners. Instead,
-- we can match the visited cloud dashboard URL against a list
-- of regexps generated by transforming `path`s from route_config
-- and ordering them in descending order of length, since a URL can
-- match multiple nested paths. We do this here using array operations
-- instead of joining on regexp_match followed by aggregation + MAX_BY
-- for efficiency reasons; route_config is small enough that all paths
-- can be stored in an array and broadcasted across the session data.
route_path_list as (
  select
    -- Array of route paths ordered by their length, from longest to shortest
    array_sort(array_agg(path), (a, b) -> length(b) - length(a)) as route_paths
  from
    perf_infra.route_config
),
route_path_pattern_list as (
  select
    -- Map route path to a matching regexp (i.e. replace parameters like `/:org_id/` with `/([^/]+)/`).
    -- We use a list of structs instead of a map to preserve the descending path length ordering.
    transform(
      route_paths, p -> named_struct('path', p, 'pattern', regexp_replace(p, ":[^/]+", "([^/]+)"))
    ) as path_patterns
  from
    route_path_list
),
timestamped_sessions_with_route_path as (
  select
   ts.*,
   -- Select the route_path that corresponds to the first
   -- regexp in the route_path_pattern_list that matches
   -- dashboard_url (URL of the visited page). Because of the
   -- array ordering, the first match will be the longest path
   -- (e.g. '/o/:org_id/workforce/activity' will match before '/o/:org_id/workforce').
   element_at(
    path_patterns,
    cast(
      array_position(
        transform(path_patterns, p -> regexp_like(dashboard_url, p.pattern)), true
      ) as int
    )
  ).path as route_path
  from timestamped_sessions ts
  cross join route_path_pattern_list
)
select
  /*+ BROADCAST(rc) */
  cast(cast(session_end_timestamp as date) as string) as date,
  uo.user_id,
  uo.org_id,
  ts.org_id as mixpanel_org_id,
  coalesce(
    uo.email,
    -- mixpanel distinct_id could be user_id (bigint) or email,
    -- and provides better coverage than mp_user_id, so favor
    -- using distinct_id as email if it is a string.
    if(
      try_cast(ts.distinct_id as bigint) is null,
      lower(ts.distinct_id),
      lower(ts.mp_user_id)
    )
  ) as user_email,
  uo.is_samsara_email,
  ts.distinct_id as mixpanel_distinct_id,
  hash(
    sha(
      concat(
        ts.distinct_id,
        cast(session_start_timestamp as string),
        cast(session_number as string)
      )
    )
  ) as session_id,
  cast(visit_timestamp as timestamp),
  visit_duration_seconds,
  cast(session_start_timestamp as timestamp),
  cast(session_end_timestamp as timestamp),
  session_duration_seconds,
  visit_order_number,
  dashboard_url,
  rc.routeName as route_name,
  rc.path as route_path,
  rc.owner as route_owner
from
  timestamped_sessions_with_route_path ts
  left join user_orgs uo on (
    case
      when try_cast(ts.distinct_id as bigint) is not null then cast(ts.distinct_id as bigint) = uo.user_id
      else coalesce(lower(ts.mp_user_id), lower(ts.distinct_id)) = uo.email
    end
  )
  and uo.org_id = cast(ts.org_id as bigint)
  left join perf_infra.route_config rc on ts.route_path = nullif(rc.path, "")
  where cast(cast(session_end_timestamp as date) as string) between '{PARTITION_START_DATE}' and '{PARTITION_END_DATE}'
"""


@table(
    database=Database.DATAMODEL_PLATFORM,
    description=build_table_description(
        table_desc="""Provides sessionized dashboard page visits from cloud dashboard users
        based on cloud route loads from mixpanel_samsara.cloud_route_session.
        A session is defined as a group of pages visited by a user, ended by at least
        30 minutes of inactivity. Each session has a session_id, which is unique per user per day.""",
        row_meaning="""Each row represents a page visited by a user during a session on the cloud dashboard.""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_hours=28,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=fct_user_session_dashboard_page_visits_schema,
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users_organizations",
        "us-west-2:mixpanel_samsara.cloud_route_session",
        "us-west-2:perf_infra.route_config",
    ],
    primary_keys=[
        "date",
        "mixpanel_distinct_id",
        "session_id",
        "visit_timestamp",
    ],
    partitioning=["date"],
    backfill_start_date="2025-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_user_session_dashboard_page_visits"),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_user_session_dashboard_page_visits",
            primary_keys=[
                "date",
                "mixpanel_distinct_id",
                "session_id",
                "visit_timestamp",
            ],
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_user_session_dashboard_page_visits",
            non_null_columns=[
                col["name"]
                for col in fct_user_session_dashboard_page_visits_schema
                if not col["nullable"]
            ],
        ),
    ],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
)
def fct_user_session_dashboard_page_visits(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START_DATE = partition_keys[0]
    PARTITION_END_DATE = partition_keys[-1]

    return fct_user_session_dashboard_page_visits_query.format(**locals())
