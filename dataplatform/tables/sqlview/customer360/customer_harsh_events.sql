  SELECT
  se.org_id,
  s.sam_number,
  se.date,
  sum(acceleration_count) AS haaccel,
  sum(braking_count) AS habraking,
  sum(harsh_turn_count) AS hasharpturn,
  sum(crash_count) AS hacrash,
  sum(generic_distraction_count + edge_distracted_driving_count) AS hadistracteddriving,
  sum(generic_tailgating_count + edge_tailgating_count) AS hatailgating
FROM report_staging.harshevent_report se
LEFT JOIN clouddb.org_sfdc_accounts osf ON se.org_id = osf.org_id
LEFT JOIN clouddb.sfdc_accounts s ON osf.sfdc_account_id = s.id
GROUP BY
  se.date,
  s.sam_number,
  se.org_id
