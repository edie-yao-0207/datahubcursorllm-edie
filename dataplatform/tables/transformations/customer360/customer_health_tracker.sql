SELECT
  cmt.date,
  cmt.org_id,
  cmt.csm_trend,
  cmt.csm_rating,
  cmt.csm_sentiment_cause,
  cmt.csm_notes
FROM dataprep.customer_health_tracking cmt
WHERE 
  cmt.date >= ${start_date} AND
  cmt.date < ${end_date} AND
  cmt.date IS NOT NULL AND
  cmt.org_id IS NOT NULL
