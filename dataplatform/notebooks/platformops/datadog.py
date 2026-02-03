# MAGIC %run /backend/platformops/aws

# COMMAND ----------

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

from datetime import datetime
import logging

import boto3

import datadog

# initialized datadog client
logger = logging.getLogger(__name__)

ssm_client = get_ssm_client("standard-read-parameters-ssm")
api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")
datadog.initialize(api_key=api_key, app_key=app_key)


def ms_between_times(start, end):
    total_time = end - start
    total_time_ms = total_time.total_seconds() * 1000

    return total_time_ms


def log_datadog_metrics(metrics):
    result = datadog.api.Metric.send(metrics=metrics)
    if result.get("errors") is not None:
        logger.warning(
            f"Error sending datadog metrics {[m['metric'] for m in metrics]}. "
            f"Code {result['code']} returned. "
            f"Errors are as follows: {result['errors']}'"
        )


def log_datadog_metric(metric, value, tags=[]):
    log_datadog_metrics([{"metric": metric, "points": value, "tags": tags}])


def log_function_duration(metric_name: str):
    """This is meant to be used as a decorator to log function duration time

    Will log the duration of the function to datadog, in milliseconds.

    Parameters
    ----------
    metric_name : str
        The name of the metric to emit to datadog

    Usage
    ------
    @log_function_duration("my_datadog_metric")
    def foo():
        return 1 + 1
    """

    def log_function_duration_decorator(func):
        def wrapper(*args, **kwargs):
            data_dog_metric = metric_name

            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()

            log_datadog_metric(data_dog_metric, ms_between_times(start_time, end_time))
            return result

        return wrapper

    return log_function_duration_decorator
