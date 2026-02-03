CREATE OR REPLACE TABLE fuel_maintenance.historical_fuel_energy_report_delta_v3
USING DELTA
PARTITIONED BY (date)
AS
SELECT * FROM report_staging.fuel_energy_efficiency_report_v3
WHERE deleted_at IS NULL
