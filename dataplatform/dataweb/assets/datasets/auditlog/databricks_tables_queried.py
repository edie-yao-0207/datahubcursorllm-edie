from dagster import AssetExecutionContext, DailyPartitionsDefinition, BackfillPolicy
from dataweb import NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context, get_all_regions
from pyspark.sql import SparkSession

SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date on which query was made"
        },
    },
    {
        "name": "event_time",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Time at which query was made"
        },
    },
    {
        "name": "table_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Table being accessed"
        },
    },
    {
        "name": "database_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Database being accessed"
        },
    },
    {
        "name": "full_table_path",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Full path of table being accessed"
        },
    },
    {
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email of user making the query"
        },
    },
    {
        "name": "action_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Action being performed by user (will be getTable)"
        },
    },
    {
        "name": "service_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Service name corresponding to query (will be unityCatalog)"
        },
    },
    {
        "name": "workspace_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Databricks workspace where query was made"
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "AWS account where query was made"
        },
    },
    {
        "name": "user_agent",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "User agent where query originated"
        },
    },
    {
        "name": "request_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Request ID of query"
        },
    },
    {
        "name": "event_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Event ID of query"
        },
    },
    {
        "name": "is_datamodel_table",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the table involved in the query belongs to the datamodel or not"
        },
    },
    {
        "name": "employee_vp",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The VP in the hierarchy of the user making query"
        },
    },
    {
        "name": "employee_manager",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Manager of user making query"
        },
    },
    {
        "name": "employee_department",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Department of user making query"
        },
    },
    {
        "name": "employee_job_family",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Job family of user making query"
        },
    },
    {
        "name": "is_r_and_d",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether employee is in R&D or not"
        },
    },
    {
        "name": "employee_user_profile",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Job family that employee belongs to (Non R&D, R&D Other, R&D Data Team, R&D Engineer, or R&D Product Manager)"
        },
    },
]

QUERY = """
    WITH tables_accessed AS (
    SELECT
            event_date AS date,
            event_time,
            split(request_params.full_name_arg, '\\\\.')[2] as table_name,
            split(request_params.full_name_arg, '\\\\.')[1] as database_name,
            request_params.full_name_arg as full_table_path,
            user_identity.email,
            action_name,
            service_name,
            workspace_id,
            account_id,
            user_agent,
            request_id,
            event_id
        FROM system.access.audit
        WHERE action_name = 'getTable'
        AND (request_params.include_browse = true OR request_params.include_browse IS NULL)
        AND response.status_code = "200"
        AND event_date BETWEEN '{FIRST_PARTITION_START}' and '{FIRST_PARTITION_END}'
    ),
    deduped_employees AS (
        SELECT *
        FROM {EMPLOYEE_HIERARCHY_PATH}.employee_hierarchy_hst
        WHERE employee_id IS NOT NULL
        AND is_active = TRUE
        AND is_worker_active = TRUE
    )

    SELECT tables_accessed.*,
        CASE WHEN tables_accessed.database_name IN (
                'dataengineering',
                'datamodel_core',
                'datamodel_telematics',
                'datamodel_safety',
                'datamodel_platform',
                'product_analytics',
                'metrics_repo',
                'metrics_api',
                'feature_store') THEN TRUE ELSE FALSE END is_datamodel_table,
        CONCAT(vp_data.first_name, ' ', vp_data.last_name) AS employee_vp,
        CONCAT(manager_data.first_name, ' ', manager_data.last_name) AS employee_manager,
        employee_data.department as employee_department,
        employee_data.job_family as employee_job_family,
        CASE WHEN employee_data.department = "100000 - Engineering" THEN TRUE ELSE FALSE END AS is_r_and_d,
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
        END AS employee_user_profile
    FROM tables_accessed
    LEFT JOIN deduped_employees employee_data
    ON lower(employee_data.employee_email) = lower(tables_accessed.email)
    LEFT JOIN deduped_employees manager_data
    ON manager_data.employee_id = employee_data.mgr_employee_id
    LEFT JOIN deduped_employees director_data
    ON manager_data.mgr_employee_id = director_data.employee_id
    LEFT JOIN deduped_employees vp_data
    ON director_data.mgr_employee_id = vp_data.employee_id
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""Table names queried by users within unity catalog.""",
        row_meaning="""A getTable request made within our Databricks workspaces""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "event_id"],
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=15,
    dq_checks=[
        PrimaryKeyDQCheck(name="dq_pk_databricks_tables_queried", primary_keys=["date", "event_id"]),
    ],
)
def databricks_tables_queried(context: AssetExecutionContext) -> str:
    context.log.info("Updating databricks_tables_queried")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]
    region = context.asset_key.path[0]
    if region == AWSRegion.US_WEST_2:
        EMPLOYEE_HIERARCHY_PATH = "edw.silver"
    else:
        EMPLOYEE_HIERARCHY_PATH = "edw_delta_share.silver"
    query = QUERY.format(
        FIRST_PARTITION_START=FIRST_PARTITION_START,
        FIRST_PARTITION_END=FIRST_PARTITION_END,
        EMPLOYEE_HIERARCHY_PATH=EMPLOYEE_HIERARCHY_PATH,
    )
    context.log.info(f"{query}")
    return query
