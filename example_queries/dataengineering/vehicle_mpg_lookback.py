queries = {
    "Lagging 28-day average MPG and its components for a set of orgs": """
      SELECT
        weighted_mpg,
        device_id,
        org_id,
        total_fuel_consumed_gallons,
        total_distance_traveled_miles,
        total_energy_consumed_kwh
      FROM dataengineering.vehicle_mpg_lookback
      WHERE date = '2024-11-06'
        AND lookback_window = '28d'
        AND org_id IN (32663, 59880, 11003099)
    """
}
