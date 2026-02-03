CREATE OR REPLACE TABLE fuel_maintenance.historical_ifta_report_delta
USING DELTA
PARTITIONED BY (date)
AS
SELECT * FROM fuel_maintenance.historical_ifta_report;
