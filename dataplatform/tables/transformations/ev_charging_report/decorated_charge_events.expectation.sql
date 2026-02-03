(
  start_ms IS NOT NULL
  AND end_ms IS NOT NULL
  AND start_ms < end_ms
)
AND (
  CASE
    WHEN start_soc IS NOT NULL
    AND end_soc IS NOT NULL
    THEN start_soc <= end_soc
  ELSE true END
)
AND (
  CASE
    WHEN energy_charged_kwh IS NOT NULL
    THEN energy_charged_kwh > 0
  ELSE true END
)
