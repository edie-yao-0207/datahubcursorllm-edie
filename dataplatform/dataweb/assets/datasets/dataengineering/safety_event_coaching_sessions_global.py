from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode
)
from dataweb.userpkgs.utils import build_table_description

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""This table contains transactions through the user coaching session funnel.""",
        row_meaning="""Each row represents a coaching session event""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
    {
      "name": "date",
      "type": "string",
      "nullable": False,
      "metadata": {"comment": "The calendar date when the coaching session started in 'YYYY-mm-dd' format"}
    },
    {
      "name": "org_id",
      "type": "long",
      "nullable": False,
      "metadata": {"comment": "The Samsara cloud dashboard ID that the data belongs to"}
    },
    {
      "name": "region",
      "type": "string",
      "nullable": False,
      "metadata": {"comment": "The Samsara cloud region that the coaching session occurred in."}
    },
    {
      "name": "id",
      "type": "string",
      "nullable": False,
      "metadata": {"comment": "A unique mixpanel id associated with the user."}
    },
    {
      "name": "session_started_time",
      "type": "long",
      "nullable": False,
      "metadata": {"comment": "The unix epoch time when the coaching session started."}
    },
    {
      "name": "session_completed_time",
      "type": "long",
      "nullable": True,
      "metadata": {"comment": "The unix epoch time when the coaching session completed."}
    }
  ],
    upstreams=[],
    primary_keys=["date", "id", "org_id", "session_started_time", "session_completed_time"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_safety_event_coaching_sessions_global"),
        NonNullDQCheck(name="dq_non_null_safety_event_coaching_sessions_global", non_null_columns=["date", "org_id", "region", "id", "session_started_time"]),
        PrimaryKeyDQCheck(name="dq_pk_safety_event_coaching_sessions_global", primary_keys=["date", "id", "org_id", "session_started_time", "session_completed_time"], block_before_write=True)
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=5,
    write_mode=WarehouseWriteMode.MERGE,
)
def safety_event_coaching_sessions_global(context: AssetExecutionContext) -> str:
    query = """
    WITH coaching_sessions_started AS (
    SELECT
        from_unixtime(a.time, 'yyyy-MM-dd') AS date,
        a.mp_date,
        COALESCE(a.mp_user_id, a.mp_device_id) AS mp_user_id,
        coalesce(a.orgid, CAST(regexp_extract(a.mp_current_url, '/o/([0-9]+)/') AS BIGINT)) as org_id,
        CASE WHEN a.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN a.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region,
        a.time AS session_started_time,
        a.distinct_id
    FROM mixpanel_samsara.fleet_safety_coaching_sessions_click_begin_sessions a -- here we grab records of users starting a coaching session
    WHERE TRUE -- arbitrary for readability
        AND COALESCE(a.mp_user_id, a.mp_device_id) NOT LIKE '%@samsara.com'
    GROUP BY 1,2,3,4,5,6,7
    ),

    -- find records of users completing a coaching session
    coaching_sessions_completed AS (
    SELECT
        from_unixtime(b.time, 'yyyy-MM-dd') AS date,
        b.mp_date,
        COALESCE(b.mp_user_id, b.mp_device_id) AS mp_user_id,
        coalesce(b.orgid, CAST(regexp_extract(b.mp_current_url, '/o/([0-9]+)/') AS BIGINT)) as org_id,
        CASE WHEN b.mp_current_url LIKE '%cloud.eu.samsara.com%' THEN 'eu-west-1' WHEN b.mp_current_url LIKE '%cloud.ca.samsara.com%' THEN 'ca-central-1' ELSE 'us-west-2' END as region,
        b.time AS session_completed_time,
        b.distinct_id
    FROM mixpanel_samsara.fleet_safety_coaching_sessions_complete_session b -- here we grab records of users completing a coaching session
    WHERE TRUE -- arbitrary for readability
        AND COALESCE(b.mp_user_id, b.mp_device_id) NOT LIKE '%@samsara.com' -- ensure we are examining non-samsara user data; can't filter on orgid because it is null in mixpanel.fleet_safety_coaching_sessions_click_begin_sessions  before 9/1/22
    GROUP BY 1,2,3,4,5,6,7
    )
    -- Now we need to find which started sessions were completed, and not abandoned.
    SELECT DISTINCT
        CAST(a.date AS STRING) AS date,
        CAST(a.org_id AS BIGINT) AS org_id,
        CAST(a.region AS STRING) AS region,
        CAST(a.distinct_id AS STRING) AS id,
        CAST(a.session_started_time AS BIGINT) AS session_started_time,
        CAST(b.session_completed_time AS BIGINT) AS session_completed_time
    FROM coaching_sessions_started a  -- here we grab records of users starting a coaching session
    LEFT JOIN coaching_sessions_completed b -- here we grab records of users completing a coaching session
        ON a.distinct_id = b.distinct_id
        AND a.org_id = b.org_id
        AND a.region = b.region
        AND a.mp_user_id = b.mp_user_id
        AND b.session_completed_time BETWEEN a.session_started_time AND (a.session_started_time + 3600) -- completed within an hour to copy logic from mixpanel
    """
    return query
