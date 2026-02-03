from dataclasses import dataclass
from enum import Enum


@dataclass
class Owner:
    team: str


DATAENGINEERING = Owner("DataEngineering")
DATAANALYTICS = Owner("DataAnalytics")
DATATOOLS = Owner("DataTools")
FIRMWAREVDP = Owner("FirmwareVDP")


class ReplicationGroups:
    RDS = "rds"
    KINESISSTATS = "kinesisstats"
    DATASTREAMS = "datastreams"
    S3BIGSTATS = "s3bigstats"
    DATAPIPELINES = "datapipelines"


class DagsterLinks:
    OncallRunBook = "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5841416/Data+Engineering+-+Oncall+Guide"
    RunPage = "https://dagster.internal.samsara.com/runs/{run_id}"
    RunsOverviewPage = "https://dagster.internal.samsara.com/runs"


org_id_default_description = "The Samsara cloud dashboard ID that the data belongs to"
group_id_default_description = "The ID of the group that the data belongs to"
sam_number_default_description = "Samnumber is a unique id that ties customer accounts together in Netsuite, Salesforce, and Samsara cloud dashboard."
device_id_default_description = "The ID of the customer device that the data belongs to"
gateway_id_default_description = (
    "The ID of the Samsara gateway that the data belongs to"
)
driver_id_default_description = "The ID of the driver that the data belongs to"
schema_version_default_description = (
    "The latest version of the database schema applied."
)
schema_version_version_default_description = "The latest version number applied."
deprecated_default_description = "[DEPRECATED] DO NOT USE"
date_default_description = "The date this row/event occurred on"
run_id_default_description = (
    "The unique identifier of the ML run ID instance for the device."
)
model_registry_key_default_description = "The unique identifier of the model."

SLACK_ALERTS_CHANNEL_DATA_ENGINEERING = "alerts-data-engineering"

UPDATED_PARTITIONS_QUERY = """
    WITH latest_partition_updates AS (
    SELECT
    created_at,
    concat_ws("|", partition_values) as partition_key,
    ROW_NUMBER() OVER (PARTITION BY partition_columns, partition_values ORDER BY created_at DESC) as rnk
    FROM auditlog.deltatable_partition_updates_log
    WHERE database LIKE '{database}'
    AND LOWER(table) = '{table}'
    AND CASE WHEN (partition_columns[0] = 'date' AND partition_values[0] >= DATE_SUB(CURRENT_DATE(), {observation_window_days})) THEN TRUE ELSE FALSE END
    )
    SELECT
        partition_key,
        created_at
    FROM latest_partition_updates
    WHERE rnk = 1
"""

# Known Exceptions raised by Databricks
CLOUD_PROVIDER_LAUNCH_EXCEPTION = "CLOUD_PROVIDER_LAUNCH_FAILURE"
AWS_INSUFFICIENT_INSTANCE_CAPACITY_EXCEPTION = (
    "AWS_INSUFFICIENT_INSTANCE_CAPACITY_FAILURE"
)
LIBRARY_EXCEPTION = "Library installation failed"
SHUFFLE_EXCEPTION = "Job aborted due to stage failure"
FAILED_READ_EXCEPTION = "FAILED_READ_FILE.NO_HINT"


# Mixpanel Schema
MIXPANEL_SCHEMA = {
    "distinct_id": "Unique identifier, user_id or email, recorded by Mixpanel.",
    "ip_address": "The ip address recorded for the Mixpanel event. Can be null.",
    "mp_browser": "Name of user's web browser determined by Mixpanel.",
    "mp_browser_version": "The Mixpanel browser version associated with the event.",
    "mp_city": "City of user determined by Mixpanel.",
    "mp_country_code": "Country code of user inferred by Mixpanel, ex: US, GB.",
    "mp_current_url": "The url of the page viewed by the user.",
    "mp_date": "Date partition of event",
    "mp_device_id": "Device id for the user's web browser assigned by Mixpanel.",
    "mp_duration": "Time (in seconds) spent on the page",
    "mp_initial_referrer": "The user's referrer to the cloud dashboard as determined by Mixpanel.",
    "mp_initial_referring_domain": "Name of website initially referring user to cloud dashboard.",
    "mp_insert_id": "Unique id of analytic event logged by Mixpanel. Primary key for table.",
    "mp_keyword": "The Mixpanel search keyword associated with the event",
    "mp_lib": "The Mixpanel library name associated with the event. Always equal to web for cloud dashboard events.",
    "mp_lib_version": "Version of Mixpanel library in use for logging event.",
    "mp_os": "The user's operating system of the event as determined by Mixpanel",
    "mp_processing_time_ms": "Time that Mixpanel processed the event in unix timestamp milliseconds.",
    "mp_referrer": "Referrer to the page",
    "mp_region": "Region of user's location recorded by Mixpanel.",
    "mp_screen_height": "Height of user's web browser determined by Mixpanel.",
    "mp_screen_width": "Screen width of the user's web browser determined by Mixpanel.",
    "mp_search_engine": "Search engine used by user preceeding page view determined by Mixpanel.",
    "mp_user_id": "The ID of the user in Mixpanel. This is similar to the useremail column. NOTE: user IDs in Mixpanel may not always be contained in this column due to how Mixpanel logs these records. To ensure the highest accuracy when it comes to finding users, coalesce mp_user_id with mp_device_id, which brings in additional IDs if they're not in the original column.",
    "orgid": "The organization id that the user was logged into for the Mixpanel event.",
    "routename": "Route name of page viewed by the user",
    "time": "Time at which event took place",
    "useremail": "Email associated with user. This can be used in different orgs so can't unique identify a user necessarily. You can filter out Samsara emails to make sure no internal events come through.",
    "utm_campaign": "The utm_campaign preceeding the page view determined by Mixpanel.",
    "utm_content": "The utm_content preceeding the page view determined by Mixpanel.",
    "utm_medium": "The Mixpanel utm_medium associated with the event.",
    "utm_source": "The utm_source preceeding the page view determined by Mixpanel.",
    "utm_term": "The utm_term recorded by Mixpanel.",
}

LANGFUSE_CLICKHOUSE_OBSERVATIONS_SCHEMA = {
    "id": "Unique observation ID (e.g. span or event identifier)",
    "trace_id": "Trace ID grouping related observations within a request",
    "parent_observation_id": "Identifier for parent span or observation, enabling nested structure",
    "type": "Observation type: GENERATION, SPAN, EVENT",
    "name": 'Descriptive name of the observation (e.g. "litellm-acompletion")',
    "start_time": "Observation start time",
    "end_time": "Observation end time (duration endpoint)",
    "completion_start_time": "Time when LLM completion began",
    "input": "Input data captured for this observation (e.g. prompt, function input)",
    "output": "Output data captured (e.g. LLM response or result)",
    "provided_model_name": 'Model name provided at observation time (e.g. "Qwen/Qwen2.5-VL-32B-Instruct")',
    "internal_model_id": "Internal/instrumentation model identifier",
    "model_parameters": "Model parameters (e.g. temperature, max_tokens)",
    "provided_usage_details": "Reported token usage (input/output) if provided",
    "usage_details": "Structured usage details (Langfuse SDK parsed)",
    "provided_cost_details": "Raw cost info if directly provided",
    "cost_details": "Structured cost breakdown as captured by Langfuse instrumentation",
    "total_cost": "Total cost incurred for the observation (double precision)",
    "prompt_id": "Identifier for a defined prompt template or prompt usage",
    "prompt_name": "Name of the prompt template (if available)",
    "prompt_version": "Prompt version (if tracked)",
    "metadata": "JSON metadata at observation level (arbitrary key-value context)",
    "level": "Log verbosity level: DEFAULT, ERROR, DEBUG",
    "status_message": "Optional status or error message to accompany level",
    "version": "Version or release of the instrumentation at observation creation",
    "project_id": "Langfuse project identifier",
    "environment": "Deployment environment (e.g. default)",
    "created_at": "Record insertion timestamp",
    "updated_at": "Record last updated timestamp",
    "event_ts": "Event timestamp as recorded in ClickHouse",
    "is_deleted": "Soft-delete flag (1 = deleted)",
    "date": "Partition date for storage",
}

LANGFUSE_CLICKHOUSE_SCORES_SCHEMA = {
    "id": "Unique score identifier",
    "timestamp": "Timestamp when the score was recorded",
    "project_id": "Langfuse project identifier",
    "environment": "Deployment environment (e.g. default)",
    "trace_id": "Trace ID that this score is associated with",
    "observation_id": "Observation ID that this score is associated with",
    "name": 'Score metric name (e.g. "time_to_first_token_seconds", "feedback_vote")',
    "value": "Numeric score value (double precision)",
    "string_value": "String representation of the score value (if applicable)",
    "data_type": 'Data type of the score: "NUMERIC", "CATEGORICAL", "BOOLEAN"',
    "source": 'Source of the score: "API" (most common)',
    "comment": "Optional comment or description for the score",
    "author_user_id": "User ID who created or provided the score",
    "config_id": "Configuration identifier for the scoring criteria",
    "queue_id": "Queue identifier for batch processing",
    "created_at": "Record creation timestamp",
    "updated_at": "Record last update timestamp",
    "event_ts": "Event timestamp from ClickHouse data source",
    "is_deleted": "Soft-delete flag: 1 = deleted",
    "date": "Partition date",
}

LANGFUSE_CLICKHOUSE_TRACES_SCHEMA = {
    "id": "Unique trace identifier (e.g. UUID or user-supplied traceId)",
    "timestamp": "Timestamp when the trace was recorded (trace start time)",
    "name": 'Descriptive name of the trace (e.g. "inbox-summ", "litellm-acompletion")',
    "user_id": "Identifier for the user associated with the trace-enables per-user observability",
    "session_id": "Session identifier grouping related traces (e.g. conversation or user flow)",
    "input": "Input to the traced operation (e.g. prompt or request payload)",
    "output": "Output of the operation (e.g. LLM response, result)",
    "metadata": "Arbitrary JSON metadata at trace level (e.g. request ID, source, version)",
    "tags": "Trace-level tags used for filtering and organization in the UI/API",
    "bookmarked": "Flag indicating if a trace was bookmarked for review or follow-up",
    "public": "Visibility flag-whether the trace is publicly shareable",
    "release": "Release or application version associated with the trace",
    "version": "Semantic version of your application or component",
    "project_id": "Langfuse project identifier",
    "environment": "Deployment environment (e.g. default)",
    "created_at": "Record creation timestamp",
    "updated_at": "Record last update timestamp",
    "event_ts": "Event timestamp from ClickHouse data source",
    "is_deleted": "Soft-delete flag: 1 = deleted",
    "date": "Partition date",
}


class GlossaryTerm(Enum):
    AI = "AI"
    BUSINESS_VALUE = "Business Value"
    MPR = "MPR"
    PLATFORM = "Platform"
    SAFETY = "Safety"
    TELEMATICS = "Telematics"


# Add new constants for custom namespaces that need special handling
DBT_PROJECT_NAMESPACES = ["databricks", "bted_edw", "MarketingAnalytics"]
