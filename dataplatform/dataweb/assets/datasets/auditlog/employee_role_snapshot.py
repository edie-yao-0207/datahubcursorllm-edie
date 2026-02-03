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
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context
from pyspark.sql import SparkSession

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Partition key for the table, in YYYY-MM-DD format"
        },
    },
    {
        "name": "workday_data_pulled_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Date from which the data was pulled from Workday"
        },
    },
    {
        "name": "workday_effective_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Effective date of employee from Workday"
        },
    },
    {
        "name": "employee_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of employee"
        },
    },
    {
        "name": "worker",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of employee"
        },
    },
    {
        "name": "worker_active",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Whether the employee is active or not"
        },
    },
    {
        "name": "department",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Department of employee"
        },
    },
    {
        "name": "evp",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "EVP of employee"
        },
    },
    {
        "name": "vp",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "VP in the hierarchy of employee"
        },
    },
    {
        "name": "director",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Director in hierarchy of employee"
        },
    },
    {
        "name": "manager",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Manager of employee"
        },
    },
    {
        "name": "job_family",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Job family that employee belongs to"
        },
    },
    {
        "name": "job_profile",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Job profile of employee"
        },
    },
    {
        "name": "businesstitle",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Business title of employee"
        },
    },
    {
        "name": "email",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Email of employee"
        },
    },
]

QUERY = """
    WITH deduped_employees AS (
        SELECT *
        FROM edw.silver.employee_hierarchy_hst
        WHERE employee_id IS NOT NULL
        AND is_active = TRUE
        AND is_worker_active = TRUE
    )
    SELECT '{FIRST_PARTITION_START}' AS date,
            CAST(emp.report_effective_date AS STRING) as workday_data_pulled_date,
            CAST(emp.effective_start_date AS STRING) as workday_effective_date,
            CAST(emp.employee_id AS STRING) as employee_id,
            CONCAT(emp.first_name, ' ', emp.last_name) as worker,
            CAST(emp.is_active AS STRING) as worker_active,
            emp.department as department,
            CONCAT(svp.first_name, ' ', svp.last_name) as evp,
            CONCAT(vp.first_name, ' ', vp.last_name) as vp,
            CONCAT(dir.first_name, ' ', dir.last_name) as director,
            CONCAT(mgr.first_name, ' ', mgr.last_name) as manager,
            emp.job_family as job_family,
            emp.job_profile as job_profile,
            emp.business_title as businesstitle,
            emp.employee_email as email
    FROM deduped_employees emp
    LEFT JOIN deduped_employees mgr ON emp.mgr_employee_id = mgr.employee_id
    LEFT JOIN deduped_employees dir ON mgr.mgr_employee_id = dir.employee_id
    LEFT JOIN deduped_employees vp ON dir.mgr_employee_id = vp.employee_id
    LEFT JOIN deduped_employees svp ON vp.mgr_employee_id = svp.employee_id
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""Snapshot of employee role data for historical purpose.""",
        row_meaning="""A snapshot of a specific employee role""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "employee_id"],
    partitioning=DailyPartitionsDefinition(start_date="2024-10-13"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonNullDQCheck(name="dq_non_null_employee_role_snapshot", non_null_columns=["date", "employee_id"]),
        PrimaryKeyDQCheck(name="dq_pk_employee_role_snapshot", primary_keys=["date", "employee_id"]),
    ],
)
def employee_role_snapshot(context: AssetExecutionContext) -> str:
    context.log.info("Updating employee_role_snapshot")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    query = QUERY.format(
                    FIRST_PARTITION_START=FIRST_PARTITION_START,
    )
    context.log.info(f"{query}")
    return query
