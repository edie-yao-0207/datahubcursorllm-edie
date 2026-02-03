import json
from datetime import datetime
from typing import List, Literal, Tuple

from dagster import HookContext, failure_hook, success_hook
from pdpyras import EventsAPISession

from .constants import DagsterLinks
from .utils import (
    DatadogMetric,
    get_run_env,
    get_ssm_token,
    initialize_datadog,
    send_datadog_metrics,
    slack_custom_alert,
)


def _get_run_type(context: HookContext):
    backfill_key = context._step_execution_context.run_tags.get("dagster/backfill")
    automaterialize_key = context._step_execution_context.run_tags.get(
        "dagster/asset_evaluation_id"
    )
    schedule_key = context._step_execution_context.run_tags.get("dagster/schedule_name")
    if backfill_key:
        run_type = "backfill"
    elif schedule_key:
        run_type = "schedule"
    elif automaterialize_key:
        run_type = "automaterialization"
    else:
        run_type = "unclassified"

    return run_type


def _get_job_type(context):
    compute_kind = context.op.tags.get("dagster/compute_kind")
    if compute_kind in ("sql", "spark"):
        job_type = "asset_materialization"
    elif compute_kind == "dq":
        job_type = "dq"
    elif compute_kind == "ddl":
        job_type = "dq"
    else:
        job_type = "unclassified"
    return job_type


def _get_partition_keys_from_hook_context(context: HookContext) -> List[str]:

    partition_keys = []

    if context._step_execution_context.has_asset_partitions_for_output("result"):
        partition_def = context._step_execution_context.partitions_def_for_output(
            "result"
        )
        partition_key_range = (
            context._step_execution_context.asset_partition_key_range_for_output(
                "result"
            )
        )
        partition_keys = partition_def.get_partition_keys_in_range(
            partition_key_range=partition_key_range
        )

    return partition_keys


def _get_op_path(context: HookContext) -> Tuple[str, str, str]:
    op_key_parts = context.op.name.split("__")
    if len(op_key_parts) == 3:
        region = op_key_parts[-3].replace("_", "-")
        database = op_key_parts[-2]
        table = op_key_parts[-1]
    else:
        region = "None"
        database = "None"
        table = op_key_parts[-1]

    return region, database, table


def emit_run_metrics(context: HookContext, status: str):
    run_type = _get_run_type(context)
    job_type = _get_job_type(context)
    region, database, table = _get_op_path(context)

    from dataweb import defs

    asset_specs = {spec.key: spec for spec in defs.get_all_asset_specs()}

    owner_map = {
        ".".join(key.path[1:]): value.owners
        for key, value in asset_specs.items()
        if value.owners
    }

    owner_map = {key: value[0] for key, value in owner_map.items()}

    table_owner = owner_map.get(f"{database}.{table}")

    if table_owner:
        table_owner = table_owner.replace("team:", "")

    context.log.info(f"table_owner: {table_owner}")

    tags = [
        f"job_type:{job_type}",
        f"run_type:{run_type}",
        f"status:{status}",
        f"name:{context.op.name}",
        f"region:{region}",
        f"database:{database}",
        f"table:{table}",
        f"owner:{table_owner}",
    ]

    now = datetime.utcnow().timestamp()
    metrics = []
    partition_keys = _get_partition_keys_from_hook_context(context) or ["unpartitioned"]

    for partition_key in partition_keys:
        dagster_run_tags = tags + [
            f"partition:{partition_key}",
        ]
        metrics.append(
            DatadogMetric(
                metric="dagster.job.run",
                type="count",
                points=[
                    (now, 1.0),
                ],
                tags=dagster_run_tags,
            )
        )

    databricks_run_id = getattr(
        context._step_execution_context.step_launcher, "_databricks_run_id", ""
    )

    try:
        databricks_run = context._step_execution_context.step_launcher.databricks_runner.client.workspace_client.jobs.get_run(
            databricks_run_id
        ).as_dict()
    except Exception:
        databricks_run = {}

    # Emit metrics for associated Databricks run, if one exists.
    if databricks_run:
        if len(partition_keys) == 1:
            partition_key = partition_keys[0]
        else:
            partition_key = f"{partition_keys[0]}_{partition_keys[-1]}"

        run_duration = databricks_run.get("end_time", 0) - databricks_run.get(
            "start_time", 0
        )
        databricks_run_tags = tags + [
            f"partition:{partition_key}",
        ]
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.duration",
                type="count",
                points=[
                    (now, run_duration),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.setup_duration",
                type="count",
                points=[
                    (now, databricks_run.get("setup_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.cleanup_duration",
                type="count",
                points=[
                    (now, databricks_run.get("cleanup_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )
        metrics.append(
            DatadogMetric(
                metric="dagster.databricks.job.run.execution_duration",
                type="count",
                points=[
                    (now, databricks_run.get("execution_duration", 0)),
                ],
                tags=databricks_run_tags,
            )
        )

    if get_run_env() == "prod":
        initialize_datadog()
        send_datadog_metrics(context, metrics=metrics)
    else:
        context.log.info(
            f"printing {status} hook metrics that would be logged on prod run"
        )
        context.log.info(f"Dagster run tags: {dagster_run_tags}")
        context.log.info(f"Databricks run tags: {databricks_run_tags}")
    return


def send_pagerduty_events(context: HookContext, action: Literal["trigger", "resolve"]):

    if _get_job_type(context) not in ["asset_materialization"]:
        return

    context.log.info(f"Sending PagerDuty '{action}' Event for run: {context.run_id}.")
    if get_run_env() != "prod":
        context.log.info("Skipping for non-prod run...")
        return

    routing_key = get_ssm_token("PAGERDUTY_DE_LOW_URGENCY_KEY")
    events_session = EventsAPISession(routing_key)

    # until we can figure out how to multiplex the pages to different teams, we will only page
    # for the databases owned by the Data Engineering team - product_analytics will be handled in a future PR
    # when we figure out how to page the VDP team directly
    # We will also revisit using Datadog to control these pages again
    databases_to_page_de_oncall_for = [
        "datamodel_core",
        "datamodel_core_silver",
        "datamodel_core_bronze",
        "datamodel_platform",
        "datamodel_platform_silver",
        "datamodel_platform_bronze",
        "datamodel_safety",
        "datamodel_safety_silver",
        "datamodel_safety_bronze",
        "datamodel_telematics",
        "datamodel_telematics_silver",
        "datamodel_telematics_bronze",
        "dataengineering",
        "auditlog",
        "feature_store",
        "inference_store",
    ]

    now = datetime.utcnow().timestamp()
    region, database, table = _get_op_path(context)
    if database not in databases_to_page_de_oncall_for:
        return

    partition_keys = _get_partition_keys_from_hook_context(context) or ["unpartitioned"]
    for partition_key in partition_keys:

        event_key = f"{region}__{database}__{table}__{partition_key}"

        if action == "trigger":
            title = f"""[{region}] [{database} {table}] Failing to materialize partition [{partition_key}]"""
            dedup_key = events_session.trigger(
                dedup_key=event_key,
                summary=title,
                custom_details={
                    "body": f"""{title} \n\nPlease consult the Data Engineering Oncall Guide below for debugging techniques and/or notify the Data Engineering (#ask-data-engineering) team if persists or need help on debugging. \n@slack-alerts-data-engineering @pagerduty-DataEngineeringLowUrgency \n\n\nDagster Run Link: {DagsterLinks.RunPage.format(run_id=context.run_id)} \n\nDagster Overview Link: {DagsterLinks.RunsOverviewPage} \n\nOncall Runbook: {DagsterLinks.OncallRunBook}""",
                    "run_id": context.run_id,
                    "event_type": "dagster_hook",
                    "timestamp": now,
                },
                source="dagster.internal.samsara.com",
                severity="error",
            )

        elif action == "resolve":
            events_session.resolve(event_key)

    return


def send_slack_notifications(context: HookContext):

    message = f"""
    [ALERT] Dagster Run Failure \n\nName: {context.op.name} \nRun Id: {context.run_id} \nRun Link: {DagsterLinks.RunPage.format(run_id=context.run_id)}
    """

    notifications = filter(
        lambda x: x.startswith("slack:"),
        json.loads(context.op.tags.get("notifications", "[]")),
    )
    for notification in notifications:
        channel = notification.split(":")[1]
        try:
            slack_custom_alert(channel=channel, message=message)
        except Exception as e:
            context.log.info(e)


@failure_hook
def step_failure_hook(context: HookContext):
    emit_run_metrics(context, "failure")
    # send_pagerduty_events(context, "trigger") # TODO: Uncomment this once we fix the pagerduty integration
    send_slack_notifications(context)


@success_hook
def step_success_hook(context: HookContext):
    emit_run_metrics(context, "success")
    # send_pagerduty_events(context, "resolve") # TODO: Uncomment this once we fix the pagerduty integration
