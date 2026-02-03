WITH
  date_table AS (
    SELECT EXPLODE(SEQUENCE(DATE(${start_date}), DATE(${end_date}), INTERVAL 1 DAY)) as date
  ),
  fct_driver_activity_timeframe AS (
    SELECT DISTINCT driver_id, org_id, date_table.date
    FROM datamodel_platform.fct_driver_activity AS activity
      INNER JOIN date_table
      ON date_sub(date_table.date, 30) < activity.date
      AND activity.date <= date_table.date
  )

SELECT
  fct_driver_activity_timeframe.date,
  driver_filtered.id,
  driver_filtered.org_id
FROM risk_overview.driver_filtered INNER JOIN fct_driver_activity_timeframe
  ON driver_filtered.id = fct_driver_activity_timeframe.driver_id
  AND driver_filtered.org_id = fct_driver_activity_timeframe.org_id
