queries = {
    "Lagging 28-day average MPG and its components for a set of orgs": """
      SELECT
        weighted_mpg,
        total_fuel_consumed_gallons,
        total_distance_traveled_miles,
        total_energy_consumed_kwh
      FROM dataengineering.vehicle_mpg_lookback_by_org
      WHERE date = '2024-11-06'
        AND lookback_window = '28d'
        AND org_id IN (32663, 59880, 11003099)
    """,
    "Compare average MPG among orgs with and without electric vehicles": """
      SELECT
        date,
        AVG(IF(total_energy_consumed_kwh > 0, weighted_mpg, NULL)) avg_mpg_with_evs,
        AVG(IF(total_energy_consumed_kwh <= 0, weighted_mpg, NULL)) avg_mpg_without_evs
      FROM dataengineering.vehicle_mpg_lookback_by_org
      WHERE lookback_window = '28d'
      GROUP BY 1
    """,
}
