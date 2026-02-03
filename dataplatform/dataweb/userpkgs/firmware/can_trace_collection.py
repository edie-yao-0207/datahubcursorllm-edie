"""
CAN Trace Collection Helpers

Shared utilities for CAN trace collection pipeline assets.
Provides constants and reusable SQL fragments for query-specific candidate tables.
"""

from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric


# Common duration constants (in milliseconds)
DURATION_5_MIN_MS = 300_000  # 5 minutes
DURATION_15_MIN_MS = 900_000  # 15 minutes

# Standard duration range for all trace collection
DURATION_5_TO_15_MIN = (DURATION_5_MIN_MS, DURATION_15_MIN_MS)

# Metrics to monitor for coverage gaps in CAN trace collection
# These are the signal types we track for telematics coverage gaps.
# NOTE: Signals added to this list are specifically ones we DON'T look for in other
# candidate queries. For example, seatbelt_state has its own dedicated query
# (fct_can_trace_seatbelt_trip_start_candidates) and is not included here.
# The mapping to OBD values is handled via fct_telematics_stat_metadata table joins.
MONITORED_METRICS = [
    KinesisStatsMetric.ODOMETER_INT_VALUE,
    KinesisStatsMetric.OSD_FUEL_CONSUMPTION_RATE_ML_PER_HOUR_INT_VALUE,
    KinesisStatsMetric.ENGINE_GAUGE_FUEL_LEVEL_PERCENT,
    KinesisStatsMetric.ENGINE_GAUGE_ENGINE_RPM,
    KinesisStatsMetric.ENGINE_MILLI_KNOTS_INT_VALUE,  # Vehicle Speed
    KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
]


def get_monitored_metric_names_sql() -> str:
    """
    Generate SQL list of monitored metric enum names for use in SQL IN clauses.
    
    Returns:
        Comma-separated string of quoted metric enum names, e.g., "'ODOMETER_INT_VALUE', 'ENGINE_GAUGE_ENGINE_RPM'"
    """
    return ", ".join([f"'{str(metric)}'" for metric in MONITORED_METRICS])
