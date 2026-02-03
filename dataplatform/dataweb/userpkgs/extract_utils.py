"""
Extract Pattern Utilities

Common utilities for selecting and formatting data extract patterns based on database type.
These utilities standardize how metrics data is extracted from different data sources.
"""

from typing import Any
from dataweb.userpkgs.constants import Database
from dataweb.userpkgs.firmware.metric import Metric
from dataweb import get_databases


# Centralized extract patterns
KINESIS_STATS_EXTRACT = """
SELECT
    date,
    time,
    org_id,
    object_id AS device_id,
    CAST({source.field} AS DOUBLE) AS value
FROM
    {source_table}
WHERE
    date BETWEEN "{date_start}" AND "{date_end}"
    AND NOT value.is_end
    AND NOT value.is_databreak
"""

DATAWEB_EXTRACT = """
SELECT
    date,
    time,
    org_id,
    device_id,
    CAST({source.field} AS DOUBLE) AS value
FROM
    {source_table}
WHERE
    date BETWEEN "{date_start}" AND "{date_end}"
"""


def select_extract_pattern(
    metric: Metric,
    kinesis_stats_extract: str = None,
    dataweb_extract: str = None,
    **format_kwargs: Any,
) -> str:
    """
    Select the appropriate extract pattern based on the metric's database type.

    Args:
        metric: The metric object containing type and database information
        kinesis_stats_extract: Optional custom extract template for KinesisStats sources.
                              Defaults to the standard KINESIS_STATS_EXTRACT.
        dataweb_extract: Optional custom extract template for DataWeb sources.
                        Defaults to the standard DATAWEB_EXTRACT.
        **format_kwargs: Additional keyword arguments for string formatting

    Returns:
        str: Formatted extract query for the given metric

    Raises:
        ValueError: If database type is unsupported
    """
    kinesis_extract = kinesis_stats_extract or KINESIS_STATS_EXTRACT
    dataweb_extract_pattern = dataweb_extract or DATAWEB_EXTRACT

    # Extract database name and table name from metric.type
    dbtable = f"{metric.type}"
    database = dbtable.split(".")[0]
    table_name = ".".join(dbtable.split(".")[1:])  # Handle multi-part table names

    # Get database mappings from get_databases()
    database_mappings = get_databases()

    # Look up the actual database name to use
    if database not in database_mappings:
        raise ValueError(f"Unknown database type: {database}")

    mapped_database = database_mappings[database]
    source_table = f"{mapped_database}.{table_name}"

    # Determine which template to use based on database type
    if database in [Database.KINESISSTATS, Database.KINESISSTATS_HISTORY]:
        extract = kinesis_extract.format(
            source=metric, source_table=source_table, **format_kwargs
        )
    elif database == Database.PRODUCT_ANALYTICS_STAGING:
        extract = dataweb_extract_pattern.format(
            source=metric, source_table=source_table, **format_kwargs
        )
    else:
        raise ValueError(f"Unsupported database type: {database}")

    return extract
