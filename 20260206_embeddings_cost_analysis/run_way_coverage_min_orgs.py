#!/usr/bin/env python3
"""
Run way_id coverage query and compute org set that covers all way_ids with MINIMUM TOTAL TRIP SECONDS.
Supports running on Databricks (uses spark.sql) or locally (uses SQLRunner + DATABRICKS_TOKEN).

Algorithm: Weighted set cover greedy - at each step, pick org with best cost-effectiveness 
(trip_seconds / new_ways_covered). This minimizes total trip seconds while covering all ways.

Date range: Jan 31 - Feb 5, 2026 (6 days)
Sampling: First 10 seconds of each hour
Device filter: CM33 only
Org filter: 
  - 10+ active CM33 devices (l7 > 0 from lifetime_device_activity)
  - is_paid_safety_customer = true (from dim_organizations)
  - Customer org only (internal_type = 0)
  - Not in dojo.ml_blocklisted_org_ids

Usage:
  - On Databricks: just run the notebook/script (spark is available automatically)
  - Locally: source .envrc && python run_way_coverage_min_orgs.py
"""

import os
import sys

# Detect if running on Databricks (spark is available in the global scope)
ON_DATABRICKS = "spark" in dir() or "DATABRICKS_RUNTIME_VERSION" in os.environ

if not ON_DATABRICKS:
    # Local execution: import SQLRunner
    backend_root = os.environ.get("BACKEND_ROOT", os.path.expanduser("~/co/backend"))
    llm_path = f"{backend_root}/go/src/samsaradev.io/infra/dataplatform/dagster/projects/datamodel/datamodel/common/llm"
    if llm_path not in sys.path:
        sys.path.insert(0, llm_path)
    from sql_runner import SQLRunner

# Date range
DATE_START = '2026-01-31'
DATE_END = '2026-02-05'

# Query 1: Get way_id coverage by org
# Filters: CM33, 10+ active devices (l7>0), paid safety customers, customer orgs, not blocklisted
WAY_COVERAGE_QUERY = f"""
WITH active_devices AS (
  -- Devices active in last 7 days (l7 > 0) as of last day of week
  SELECT device_id, org_id
  FROM datamodel_core.lifetime_device_activity
  WHERE date = '{DATE_END}'
    AND l7 > 0
),
active_cm33_orgs AS (
  -- Orgs with 10+ active CM33 devices
  SELECT d.org_id, COUNT(DISTINCT d.device_id) AS cm33_count
  FROM datamodel_core.dim_devices d
  JOIN datamodel_core.dim_organizations o
    ON d.org_id = o.org_id
    AND d.date = o.date
    AND o.internal_type = 0  -- customer org only
  JOIN active_devices ad
    ON d.device_id = ad.device_id
    AND d.org_id = ad.org_id
  WHERE d.date = '{DATE_END}'
    AND d.associated_devices.camera_product_id = 167  -- CM33 only
    AND o.date = '{DATE_END}'
    AND o.is_paid_safety_customer = true
    AND o.org_id NOT IN (SELECT org_id FROM dojo.ml_blocklisted_org_ids)
  GROUP BY d.org_id
  HAVING COUNT(DISTINCT d.device_id) >= 10
),
cm33_devices AS (
  -- Get all active CM33 devices from qualified orgs
  SELECT DISTINCT d.device_id, d.org_id
  FROM datamodel_core.dim_devices d
  INNER JOIN active_cm33_orgs ao ON d.org_id = ao.org_id
  INNER JOIN active_devices ad ON d.device_id = ad.device_id
  WHERE d.date = '{DATE_END}'
    AND d.associated_devices.camera_product_id = 167
),
way_coverage AS (
  -- Get way_ids covered by each org
  SELECT
    cd.org_id,
    l.value.way_id AS way_id
  FROM kinesisstats.location l
  INNER JOIN cm33_devices cd
    ON l.device_id = cd.device_id
  WHERE
    l.date BETWEEN '{DATE_START}' AND '{DATE_END}'
    AND minute(from_unixtime(l.time / 1000)) = 0
    AND second(from_unixtime(l.time / 1000)) < 10  -- first 10 seconds of each hour
    AND l.value.way_id IS NOT NULL
  GROUP BY cd.org_id, l.value.way_id
)
SELECT org_id, way_id FROM way_coverage
"""

# Query 2: Trip seconds per org for the same week (CM33 only, qualified orgs)
TRIP_SECONDS_QUERY = f"""
WITH active_devices AS (
  SELECT device_id, org_id
  FROM datamodel_core.lifetime_device_activity
  WHERE date = '{DATE_END}'
    AND l7 > 0
),
active_cm33_orgs AS (
  SELECT d.org_id
  FROM datamodel_core.dim_devices d
  JOIN datamodel_core.dim_organizations o
    ON d.org_id = o.org_id
    AND d.date = o.date
    AND o.internal_type = 0
  JOIN active_devices ad
    ON d.device_id = ad.device_id
    AND d.org_id = ad.org_id
  WHERE d.date = '{DATE_END}'
    AND d.associated_devices.camera_product_id = 167
    AND o.date = '{DATE_END}'
    AND o.is_paid_safety_customer = true
    AND o.org_id NOT IN (SELECT org_id FROM dojo.ml_blocklisted_org_ids)
  GROUP BY d.org_id
  HAVING COUNT(DISTINCT d.device_id) >= 10
)
SELECT
  t.org_id,
  SUM((t.end_time_ms - t.start_time_ms) / 1000) AS trip_seconds
FROM datamodel_telematics.fct_trips t
INNER JOIN datamodel_core.dim_devices cm
  ON t.org_id = cm.org_id
  AND t.device_id = cm.associated_devices.vg_device_id
  AND t.date = cm.date
  AND cm.product_name = 'CM33'
INNER JOIN active_cm33_orgs ao
  ON t.org_id = ao.org_id
WHERE t.date BETWEEN '{DATE_START}' AND '{DATE_END}'
  AND t.start_time_ms < t.end_time_ms
GROUP BY t.org_id
"""

# Query to count qualified orgs
COUNT_ORGS_QUERY = f"""
WITH active_devices AS (
  SELECT device_id, org_id
  FROM datamodel_core.lifetime_device_activity
  WHERE date = '{DATE_END}'
    AND l7 > 0
),
active_cm33_orgs AS (
  SELECT d.org_id, COUNT(DISTINCT d.device_id) AS cm33_count
  FROM datamodel_core.dim_devices d
  JOIN datamodel_core.dim_organizations o
    ON d.org_id = o.org_id
    AND d.date = o.date
    AND o.internal_type = 0
  JOIN active_devices ad
    ON d.device_id = ad.device_id
    AND d.org_id = ad.org_id
  WHERE d.date = '{DATE_END}'
    AND d.associated_devices.camera_product_id = 167
    AND o.date = '{DATE_END}'
    AND o.is_paid_safety_customer = true
    AND o.org_id NOT IN (SELECT org_id FROM dojo.ml_blocklisted_org_ids)
  GROUP BY d.org_id
  HAVING COUNT(DISTINCT d.device_id) >= 10
)
SELECT COUNT(*) AS num_orgs, SUM(cm33_count) AS total_cm33_devices
FROM active_cm33_orgs
"""


def min_cost_org_set_greedy(way_df, trip_df):
    """
    Weighted set cover: find org set that covers all way_ids with minimum total trip seconds.
    
    Greedy approach: at each step, pick org with best cost-effectiveness ratio:
    trip_seconds / num_new_ways_covered (lower is better).
    """
    # Build org_id -> set of way_ids
    org_to_ways = {}
    for _, row in way_df.iterrows():
        org_id = row["org_id"]
        way_id = row["way_id"]
        org_to_ways.setdefault(org_id, set()).add(way_id)

    # Build org_id -> trip_seconds
    org_to_cost = {}
    for _, row in trip_df.iterrows():
        org_to_cost[row["org_id"]] = row["trip_seconds"]

    # Only consider orgs that have both coverage AND trip data
    valid_orgs = set(org_to_ways.keys()) & set(org_to_cost.keys())
    print(f"Orgs with way coverage: {len(org_to_ways)}")
    print(f"Orgs with trip data: {len(org_to_cost)}")
    print(f"Orgs with both (valid): {len(valid_orgs)}")

    all_ways = frozenset().union(*org_to_ways.values())
    print(f"Total unique way_ids to cover: {len(all_ways):,}")
    
    covered = set()
    chosen_orgs = []
    total_cost = 0

    iteration = 0
    while covered < all_ways:
        best_org = None
        best_ratio = float('inf')

        for org_id in valid_orgs:
            if org_id in chosen_orgs:
                continue
            new_ways = org_to_ways.get(org_id, set()) - covered
            if len(new_ways) == 0:
                continue
            cost = org_to_cost.get(org_id, float('inf'))
            ratio = cost / len(new_ways)
            if ratio < best_ratio:
                best_ratio = ratio
                best_org = org_id

        if best_org is None:
            remaining = all_ways - covered
            print(f"\nWARNING: {len(remaining)} way_ids cannot be covered by orgs with trip data.")
            break

        chosen_orgs.append(best_org)
        new_covered = org_to_ways[best_org] - covered
        covered |= org_to_ways[best_org]
        total_cost += org_to_cost[best_org]
        
        iteration += 1
        if iteration % 50 == 0:
            print(f"  Iteration {iteration}: {len(covered):,}/{len(all_ways):,} ways covered ({100*len(covered)/len(all_ways):.1f}%)")

    return chosen_orgs, total_cost, len(all_ways)


def execute_query(query, runner=None):
    """Execute a SQL query and return a pandas DataFrame.
    Uses spark.sql() on Databricks, SQLRunner locally.
    """
    if ON_DATABRICKS:
        return spark.sql(query).toPandas()  # noqa: F821 - spark is available on DBX
    else:
        return runner.execute_query(query)


def main():
    print(f"=== Way ID Coverage Analysis ===")
    print(f"Date range: {DATE_START} to {DATE_END}")
    print(f"Sampling: First 10 seconds of each hour")
    print(f"Device filter: CM33 only")
    print(f"Org filter: 10+ active CM33, paid safety customers, customer orgs")
    print(f"Runtime: {'Databricks' if ON_DATABRICKS else 'Local (SQLRunner)'}")

    runner = None
    if not ON_DATABRICKS:
        if not os.environ.get("DATABRICKS_TOKEN"):
            print("ERROR: DATABRICKS_TOKEN not set. Run: source .envrc  (or direnv allow)")
            sys.exit(1)
        runner = SQLRunner(warehouse_id="9fb6a34db2b0bbde", region="us")

    print("\nCounting qualified orgs...")
    count_df = execute_query(COUNT_ORGS_QUERY, runner)
    print(f"Qualified orgs: {count_df['num_orgs'].iloc[0]:,}")
    print(f"Total CM33 devices: {count_df['total_cm33_devices'].iloc[0]:,}")

    print("\nFetching way coverage data...")
    way_df = execute_query(WAY_COVERAGE_QUERY, runner)
    if way_df.empty:
        print("No way coverage data returned.")
        return

    print("Fetching trip seconds data...")
    trip_df = execute_query(TRIP_SECONDS_QUERY, runner)
    if trip_df.empty:
        print("No trip data returned.")
        return

    print("\nRunning weighted set cover (minimize total trip seconds)...\n")
    chosen_orgs, total_cost, total_ways = min_cost_org_set_greedy(way_df, trip_df)

    print(f"\n=== RESULT ===")
    print(f"Total unique way_ids covered: {total_ways:,}")
    print(f"Org list that covers all way_ids with minimum trip seconds:")
    print(chosen_orgs)
    print(f"\nTotal orgs: {len(chosen_orgs)}")
    print(f"Total trip seconds: {total_cost:,.0f}")
    print(f"Total trip hours: {total_cost / 3600:,.1f}")


if __name__ == "__main__":
    main()
