# Databricks notebook source

# COMMAND ----------
from enum import Enum
import requests
from typing import Dict, Any, List, Tuple, NamedTuple, Optional, Union
from datetime import datetime, timezone, timedelta
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

# Constants for duration calculations
MILLISECONDS_PER_WEEK = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds

MILES_PER_METER = 0.000621371

DESTINATION_TABLE = "dataplatform_dev.benchmark_metrics_per_org"


class RunModes(Enum):
    TEST = "test"
    BENCHMARKS_BETA_ORGS_ONLY = "benchmarks_beta_orgs_only"
    ALL_CUSTOMERS_ORGS = "all_customers_orgs"


class MonthsLookback(Enum):
    THREE = 3
    SIX = 6
    TWELVE = 12


class AuthCredentials(NamedTuple):
    cookie: str
    csrf_token: str


class AuthorizationError(Exception):
    """Raised when a GraphQL request fails due to authorization issues."""

    pass


def get_duration_ms(duration: Union[str, MonthsLookback], end_ms: int) -> int:
    """
    Get duration in milliseconds based on either:
    1. Number of months using MonthsLookback enum (3, 6, or 12 months)
    2. A start date string in format 'YYYY-MM-DD'

    Args:
        duration: Either MonthsLookback enum or start date string
        end_ms: End time in milliseconds to calculate duration from if start date provided

    Returns:
        Duration in milliseconds

    Raises:
        ValueError: If start date string is not in correct format
    """
    if isinstance(duration, MonthsLookback):
        durations = {
            3: 7498800000,  # ~87 days
            6: 14756400000,  # ~171 days
            12: 31690800000,  # ~367 days
        }
        return durations[duration.value]
    else:
        # Parse the start date string into a datetime object with UTC timezone
        try:
            start_time = datetime.strptime(duration, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
            start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
            start_ms = int(start_time.timestamp() * 1000)
            return end_ms - start_ms
        except ValueError:
            raise ValueError("Start date must be in format 'YYYY-MM-DD'")


def get_graphql_response(
    graphql_query: str,
    variables: Dict[str, Any],
    query_name: str,
    auth: AuthCredentials,
) -> Dict[str, Any]:
    """
    Make a GraphQL request to the Samsara API.

    Raises:
        AuthorizationError: If the request fails due to authorization issues
    """
    body = {
        "query": graphql_query,
        "variables": variables,
        "extensions": {
            "route": "/debug/graphql",
            "stashOutput": True,
            "storeDepSet": True,
        },
    }

    resp = requests.post(
        f"https://sf1-ws.cloud.samsara.com/r/graphql?q={query_name}",
        json=body,
        headers={"Cookie": auth.cookie, "X-CSRF-TOKEN": auth.csrf_token},
    )

    response_data = resp.json()

    # Check for authorization errors
    if response_data.get("error") == "Authorization Failed":
        raise AuthorizationError(
            f"Authorization failed for query {query_name}. "
            f"Request ID: {response_data.get('metadata', {}).get('requestId')}, "
            f"Status: {response_data.get('metadata', {}).get('status')}"
        )

    return response_data


def get_beta_org_ids() -> List[int]:
    """
    Get list of organization IDs that are in the fleet benchmarks beta.

    Returns:
        List[int]: List of organization IDs
    """
    org_df = spark.sql(
        """
        select
            org_id
        from
            dataanalytics_dev.fleet_benchmarks_orgs_in_beta
        where internal_type_name = 'Customer Org'
    """
    )

    return [row.org_id for row in org_df.collect()]


def get_customers_org_ids() -> List[int]:
    """
    Get list of organization IDs that are all customers.

    Returns:
        List[int]: List of organization IDs
    """
    org_df = spark.sql(
        """
        select
            org_id
        from
           datamodel_core.dim_organizations
        where
            date = (select max(date) from datamodel_core.dim_organizations)
            and internal_type_name = 'Customer Org'
            and is_paid_customer = true
    """
    )

    return [row.org_id for row in org_df.collect()]


def write_results_to_delta_table(
    results,
    run_uuid,
    test_mode=True,
    end_ms=None,
    duration_ms=None,
    gql_request_times=None,
):
    """
    Convert results to DataFrame and write to Delta table.
    Handles None values by converting them to NULL in the Delta table.

    Args:
        results: List of tuples containing (org_id, metric_type, metric_value)
        test_mode: If True, just print results instead of writing to Delta
        end_ms: End time in milliseconds used in the GraphQL query
        duration_ms: Duration in milliseconds used in the GraphQL query
        gql_request_times: Dictionary mapping org_id to GraphQL request timestamp
    """
    # Get current UTC timestamp
    current_time = datetime.now(timezone.utc)

    schema = StructType(
        [
            StructField("org_id", LongType(), False),
            StructField("metric_type", StringType(), False),
            StructField("metric_value", DoubleType(), True),  # Allow NULL values
            StructField("end_ms", LongType(), False),
            StructField("duration_ms", LongType(), False),
            StructField("_gql_request_time", TimestampType(), False),
            StructField("_row_inserted_at", TimestampType(), False),
            StructField("_run_uuid", StringType(), False),
        ]
    )

    # Convert results to DataFrame with explicit schema
    # Handle None values by converting them to NULL
    results_df = spark.createDataFrame(
        [
            (
                int(org_id),
                str(metric_type),
                float(value) if value is not None else None,
                end_ms,
                duration_ms,
                gql_request_times.get(org_id),
                current_time,
                run_uuid,
            )
            for org_id, metric_type, value in results
        ],
        schema=schema,
    )

    # Just print results in test mode
    if test_mode:
        print("\nTest Results:")
        results_df.show()
    else:
        # Write results to Delta table
        results_df.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(DESTINATION_TABLE)


def truncate_destination_table(run_uuid: str):
    spark.sql(f"TRUNCATE TABLE {DESTINATION_TABLE} WHERE _run_uuid = '{run_uuid}'")


def _process_severe_speeding(org_data: Dict[str, Any]) -> Optional[float]:
    """
    Process severe speeding data from GraphQL response.
    Returns None if required data is missing.
    """

    datapoints = org_data["speedingReportTrends"]["datapoints"]

    if not datapoints:
        return None

    cumulative_severe_speeding_ms = 0
    cumulative_total_ms = 0

    for data_point in datapoints:
        if "speedingSummary" not in data_point:
            continue

        summary = data_point["speedingSummary"]
        try:
            total_ms = (
                summary["notSpeedingMs"]
                + summary["lightSpeedingMs"]
                + summary["moderateSpeedingMs"]
                + summary["heavySpeedingMs"]
                + summary["severeSpeedingMs"]
            )

            severe_speeding_ms = summary["severeSpeedingMs"]
            cumulative_severe_speeding_ms += severe_speeding_ms
            cumulative_total_ms += total_ms
        except (KeyError, TypeError):
            continue

    if cumulative_total_ms == 0:
        return None

    severe_speeding = cumulative_severe_speeding_ms / cumulative_total_ms * 100
    return severe_speeding


def _process_mobile_usage(org_data: Dict[str, Any]) -> Optional[float]:
    """
    Process mobile usage data from GraphQL response.
    Returns None if required data is missing.
    """
    datapoints = org_data["harshEventsReportTrends"]["datapoints"]

    if not datapoints:
        return None

    cumulative_mobile_usage_count = 0
    cumulative_distance_meters = 0

    for datapoint in datapoints:
        try:
            mobile_usage_count = datapoint["behaviorLabelCounts"]["mobileUsageCount"]
            distance_meters = datapoint["distanceMeters"]

            cumulative_mobile_usage_count += mobile_usage_count
            cumulative_distance_meters += distance_meters
        except (KeyError, TypeError):
            continue

    if cumulative_distance_meters == 0:
        return None

    mobile_usage = (cumulative_mobile_usage_count * 1000) / (
        MILES_PER_METER * cumulative_distance_meters
    )
    return mobile_usage


def _process_mpge(org_data: Dict[str, Any]) -> Optional[float]:
    """
    Process MPGe data from GraphQL response.
    Returns None if required data is missing.
    """
    report = org_data["fuelEnergyEfficiencyAggregateByVehicle"]["report"]

    if not report or "efficiencyMpge" not in report:
        return None

    try:
        mpge = float(report["efficiencyMpge"])
        return mpge
    except (ValueError, TypeError):
        return None


def _process_idling(org_data: Dict[str, Any]) -> Optional[float]:
    """
    Process idling data from GraphQL response.
    Returns None if required data is missing.
    """
    datapoints = org_data["fuelEnergyEfficiencyIntervalTrends"]["datapoints"]

    if not datapoints:
        return None

    cumulative_idle_ms = 0
    cumulative_on_ms = 0

    for datapoint in datapoints:
        try:
            idle_ms = datapoint["idleDurationMs"]
            on_ms = datapoint["onDurationMs"]

            cumulative_idle_ms += idle_ms
            cumulative_on_ms += on_ms
        except (KeyError, TypeError):
            continue

    if cumulative_on_ms == 0:
        return None

    idling_percent = cumulative_idle_ms / cumulative_on_ms * 100
    return idling_percent


def get_all_metrics_for_org(
    org_id: int, end_ms: int, duration_ms: int, auth: AuthCredentials
) -> Tuple[List[Tuple[int, str, Optional[float]]], datetime]:
    """
    Get all metrics for a single organization using a single GraphQL request.

    Args:
        org_id: Organization ID
        end_ms: End time in milliseconds
        duration_ms: Duration to look back in milliseconds
        auth: Authentication credentials

    Returns:
        Tuple of:
        - List of tuples (org_id, metric_type, value) where value can be None
        - Timestamp when the GraphQL request was made

    Raises:
        AuthorizationError: If the request fails due to authorization issues
    """
    # Get current UTC timestamp before making GraphQL request
    gql_request_time = datetime.now(timezone.utc)

    # Combined GraphQL query for all metrics
    query = """
    query BenchmarkMetricsFromDatabricksNotebook($orgId: int64!, $durationMs: int64!, $endMs: int64!) {
      organization(id: $orgId) {
        # Speeding data
        speedingReportTrends(
          durationMs: $durationMs, 
          endMs: $endMs, 
          reportType: VehicleReportType, 
          aggregate: true, 
          timezone: "America/New_York", 
          groupByInterval: GROUP_BY_WEEK, 
          unit: MilesPerHour
        ) {
          datapoints {
            startMs
            endMs
            speedingSummary {
              notSpeedingMs
              lightSpeedingMs
              moderateSpeedingMs
              heavySpeedingMs
              severeSpeedingMs
            }
          }
        }
        
        # Mobile usage data
        harshEventsReportTrends(
          durationMs: $durationMs, 
          endMs: $endMs, 
          reportType: VEHICLE_REPORT_TYPE, 
          timezone: "America/New_York", 
          aggregate: true, 
          groupByInterval: GROUP_BY_WEEK
        ) {
          datapoints {
            startMs
            endMs
            distanceMeters
            behaviorLabelCounts {
              mobileUsageCount
            }
          }
        }
        
        # MPGe data
        fuelEnergyEfficiencyAggregateByVehicle(
          durationMs: $durationMs, 
          endAtMs: $endMs
        ) {
          report {
            efficiencyMpge
            electricityConsumedKwh
            fuelConsumptionGallons
            gaseousFuelConsumedKg
            totalDistanceDrivenMiles
          }
        }
        
        # Idling data
        fuelEnergyEfficiencyIntervalTrends(
          durationMs: $durationMs, 
          endMs: $endMs, 
          reportType: VehicleReport
        ) {
          datapoints {
            startMs
            endMs
            onDurationMs
            idleDurationMs
            auxDuringIdleMs
          }
        }
      }
    }
    """

    try:
        variables = {
            "orgId": org_id,
            "durationMs": duration_ms,
            "endMs": end_ms,
        }

        resp = get_graphql_response(
            query, variables, "BenchmarkMetricsFromDatabricksNotebook", auth
        )
        results = []

        if resp.get("output") and resp["output"].get("organization"):
            org_data = resp["output"]["organization"]

            for metric_type, metric_processor in [
                ("SEVERE_SPEEDING", _process_severe_speeding),
                ("MOBILE_USAGE", _process_mobile_usage),
                ("MPGE", _process_mpge),
                ("IDLING", _process_idling),
            ]:
                result = metric_processor(org_data)
                results.append((org_id, metric_type, result))

        return results, gql_request_time

    except AuthorizationError as e:
        print(f"\nAUTHORIZATION ERROR: {str(e)}")
        print("The script will now exit due to authorization failure.")
        raise
    except Exception as e:
        print(f"Error processing metrics for org {org_id}: {str(e)}")
        return (
            [],
            gql_request_time,
        )  # Return empty list and request time for failed organizations


def process_all_orgs(
    org_ids: List[int], end_ms: int, duration_ms: int, auth: AuthCredentials
) -> Tuple[List[Tuple[int, str, Optional[float]]], Dict[int, datetime]]:
    """
    Process all organizations using thread pool for parallelization.
    Each organization is processed with a single GraphQL request that fetches all metrics.
    Failed organizations are skipped entirely.

    Returns:
        Tuple of:
        - List of tuples (org_id, metric_type, value)
        - Dictionary mapping org_id to GraphQL request timestamp
    """
    max_workers = 16
    total_orgs = len(org_ids)
    results = []
    completed_orgs = set()
    failed_orgs = set()
    gql_request_times = {}  # Dictionary to store request times per org

    print(f"\nProcessing {total_orgs} organizations")
    print(f"Using {max_workers} worker threads\n")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_org = {
            executor.submit(
                get_all_metrics_for_org, org_id, end_ms, duration_ms, auth
            ): org_id
            for org_id in org_ids
        }

        for future in as_completed(future_to_org):
            org_id = future_to_org[future]
            completion_time = datetime.now(timezone.utc)
            try:
                org_results, request_time = future.result()
                processing_time = (completion_time - request_time).total_seconds()

                if org_results:  # Only extend results if we got valid data
                    results.extend(org_results)
                    completed_orgs.add(org_id)
                    gql_request_times[org_id] = request_time
                    status = "succeeded"
                else:
                    failed_orgs.add(org_id)
                    status = "failed"

                total_processed = len(completed_orgs) + len(failed_orgs)
                print(
                    f"Progress: {total_processed}/{total_orgs} organizations processed "
                    f"({len(completed_orgs)} succeeded, {len(failed_orgs)} failed) "
                    f"({(total_processed/total_orgs*100):.1f}%) - "
                    f"Org {org_id} {status} in {processing_time:.2f} seconds"
                )

            except AuthorizationError:
                for f in future_to_org:
                    f.cancel()
                raise
            except Exception as e:
                failed_orgs.add(org_id)
                print(f"Error processing org {org_id}: {str(e)}")

    print(f"\nCompleted processing {len(completed_orgs)}/{total_orgs} organizations")
    if failed_orgs:
        print(
            f"Failed to process {len(failed_orgs)} organizations: {sorted(failed_orgs)}"
        )

    return results, gql_request_times


def get_end_ms(end_date_time_str: str) -> int:
    """
    Get end time in milliseconds for the given date string.
    """
    # Parse the date string into a datetime object with UTC timezone
    try:
        end_time = datetime.strptime(end_date_time_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise ValueError("end_date_time_str must be a date in format 'YYYY-MM-DD'")

    end_time = end_time.replace(minute=0, second=0, microsecond=0)
    end_ms = int(end_time.timestamp() * 1000)
    return end_ms


# COMMAND ----------

dbutils.widgets.dropdown(
    "RUN_MODE", RunModes.TEST.value, [mode.value for mode in RunModes]
)
RUN_MODE: RunModes = RunModes(dbutils.widgets.get("RUN_MODE"))

# Set the end time for metrics calculation
dbutils.widgets.text("END_TIME", "2025-02-19")  # "2025-02-19"
END_TIME: str = dbutils.widgets.get("END_TIME")

# Set the number of months to look back (3, 6, or 12), or a custom duration in milliseconds
# DURATION: Union[str, MonthsLookback] = MonthsLookback.THREE
dbutils.widgets.text("DURATION", "2025-01-21")  # "2025-01-21"
DURATION: Union[str, MonthsLookback] = dbutils.widgets.get("DURATION")
# DURATION: Union[str, MonthsLookback]= MonthsLookback.THREE

test_org_id = 2000042

# Generate a unique UUID for this run
run_uuid = str(uuid.uuid4())
print(f"\nGenerated run UUID: {run_uuid}")


auth_credentials = AuthCredentials(
    cookie=dbutils.secrets.get(
        "calculate_benchmarks_metric_script_graphql_keys", "graphql_cookie"
    ),
    csrf_token=dbutils.secrets.get(
        "calculate_benchmarks_metric_script_graphql_keys", "graphql_csrf"
    ),
)

end_ms = get_end_ms(END_TIME)
duration_ms = get_duration_ms(DURATION, end_ms)

print(
    f"Using end time: {datetime.fromtimestamp(end_ms/1000).strftime('%Y-%m-%d %H:00:00 %Z')}"
)
if isinstance(DURATION, MonthsLookback):
    print(
        f"Looking back {DURATION.value} months ({duration_ms/MILLISECONDS_PER_WEEK:.0f} weeks)"
    )
else:
    start_ms = end_ms - duration_ms
    print(
        f"Looking back from {datetime.fromtimestamp(start_ms/1000).strftime('%Y-%m-%d')} ({duration_ms/MILLISECONDS_PER_WEEK:.0f} weeks)"
    )

# Get organization IDs
if RUN_MODE == RunModes.TEST:
    print(f"Running in TEST MODE with org_id = {test_org_id}")
    org_ids = [test_org_id]
elif RUN_MODE == RunModes.BENCHMARKS_BETA_ORGS_ONLY:
    print("Running in BENCHMARKS_BETA_ORGS_ONLY MODE")
    print(f"First validating Delta table write with test org {test_org_id}...")
    test_results, test_gql_request_times = process_all_orgs(
        [test_org_id], end_ms, duration_ms, auth_credentials
    )
    write_results_to_delta_table(
        test_results,
        run_uuid,
        test_mode=False,  # Important: set to False to actually write to Delta
        end_ms=end_ms,
        duration_ms=duration_ms,
        gql_request_times=test_gql_request_times,
    )
    print("✓ Successfully validated Delta table write with test org")

    print(
        f"Truncating destination table for this run ({run_uuid}) to not include test org..."
    )
    truncate_destination_table(run_uuid)
    print("✓ Successfully truncated destination table")

    print("\nFetching organization IDs from beta table...")
    org_ids = get_beta_org_ids()
    print(f"Found {len(org_ids)} organizations to process")
elif RUN_MODE == RunModes.ALL_CUSTOMERS_ORGS:
    print("Running in ALL_CUSTOMERS_ORGS MODE")
    print(f"First validating Delta table write with test org {test_org_id}...")
    test_results, test_gql_request_times = process_all_orgs(
        [test_org_id], end_ms, duration_ms, auth_credentials
    )
    write_results_to_delta_table(
        test_results,
        run_uuid,
        test_mode=False,  # Important: set to False to actually write to Delta
        end_ms=end_ms,
        duration_ms=duration_ms,
        gql_request_times=test_gql_request_times,
    )
    print("✓ Successfully validated Delta table write with test org")

    print(
        f"Truncating destination table for this run ({run_uuid}) to not include test org..."
    )
    truncate_destination_table(run_uuid)
    print("✓ Successfully truncated destination table")

    print("\nFetching all customer organization IDs ...")
    org_ids = get_customers_org_ids()
    print(f"Found {len(org_ids)} organizations to process")
else:
    raise ValueError(f"Invalid run mode: {RUN_MODE}")

# Process all organizations (if in test mode or validation succeeded)
results, gql_request_times = process_all_orgs(
    org_ids, end_ms, duration_ms, auth_credentials
)

write_results_to_delta_table(
    results, run_uuid, RUN_MODE == RunModes.TEST, end_ms, duration_ms, gql_request_times
)

print(
    "Completed processing"
    + (
        " test organization."
        if RUN_MODE == RunModes.TEST
        else " all organizations and writing results to Delta table."
    )
)

if RUN_MODE != RunModes.TEST:
    print(f"\nQuery results using run UUID: {run_uuid}")
