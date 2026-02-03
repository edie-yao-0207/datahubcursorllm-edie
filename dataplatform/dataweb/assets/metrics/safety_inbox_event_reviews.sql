WITH ser_job_event_lookup  AS (
  SELECT *
  FROM (
    SELECT
      a.job_uuid,
      a.date,
      SPLIT_PART(a.source_id, ',', 1) AS org_id,
      SPLIT_PART(a.source_id, ',', 2) AS device_id,
      SPLIT_PART(a.source_id, ',', 3) AS event_ms,
      b.updated_at,
      a.accel_type
    FROM safetyeventreviewdb.review_request_metadata a -- THIS FINDS INFORMATION ON THE EVENT TYPES
    INNER JOIN safetyeventreviewdb.jobs b -- THIS FINDS INFORMATION ON THE SER REVIEW JOBS
      ON a.job_uuid = b.uuid
      AND a.date = b.date
    INNER JOIN datamodel_core.dim_organizations o -- THIS ENSURES NON-INTERNAL ORG DATA
      ON a.org_id = o.org_id
      AND a.date = o.date
    WHERE
      o.internal_type = 0 -- INDICATES A NON-INTERNAL ORG
      AND o.account_first_purchase_date IS NOT NULL -- INDICATES A CUSTOMER PURCHASE
      AND a.queue_name = 'CUSTOMER' -- ONLY NON-TRAINING REVIEWS
      AND b.completed_at IS NOT NULL -- ONLY COMPLETED REVIEWS
    GROUP BY 1,2,3,4,5,6,7 -- LIKE "SELECT DISTINCT", BUT FASTER
  ) c
  QUALIFY ROW_NUMBER() OVER(PARTITION BY org_id, device_id, event_ms, date ORDER BY updated_at DESC) = 1 -- ONLY TAKE MOST RECENT COMPLETED JOB
),
org_categories AS (
  SELECT
    ocg.org_id,
    sam_map.sam_number,
    ocg.internal_type,
    ocg.avg_mileage,
    ocg.region,
    ocg.fleet_size,
    ocg.industry_vertical,
    ocg.industry_vertical_raw AS account_industry,
    ocg.fuel_category,
    ocg.primary_driving_environment,
    ocg.fleet_composition,
    ocg.is_paid_customer,
    ocg.is_paid_safety_customer,
    ocg.is_paid_stce_customer,
    ocg.is_paid_telematics_customer,
    ocg.account_size_segment,
    ocg.account_arr_segment
  FROM product_analytics_staging.stg_organization_categories_global ocg
  LEFT JOIN product_analytics.map_org_sam_number_latest_global sam_map
    ON ocg.org_id = sam_map.org_id
  WHERE ocg.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_global)
)
SELECT
  a.event_id,
  a.date,
  a.org_id,
  a.detection_type,
  o.sam_number,
  o.internal_type,
  a.account_size_segment,
  a.account_industry,
  a.account_arr_segment,
  a.has_ser_license,
  a.region,
  o.avg_mileage,
  o.region AS subregion,
  o.fleet_size,
  o.industry_vertical,
  o.fuel_category,
  o.primary_driving_environment,
  o.fleet_composition,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  a.has_safety_event_review,
  a.is_customer_dismissed,
  a.is_coached,
  a.is_viewed,
  CASE
    WHEN coaching_status IN ('Manual Review Dismissed') -- NOT in safety inbox
    THEN 1
    ELSE 0
  END AS is_manual_dismissed,
  CASE
    WHEN coaching_status IN ('Pending') -- NOT in safety inbox
    THEN 1
    ELSE 0
  END AS is_pending,
  CASE -- auto dismissed events
    WHEN coaching_status IN ('Auto-Dismissed')
    THEN 1
    ELSE 0
  END AS is_auto_dismissed
FROM dataengineering.safety_inbox_events_status_global a
INNER JOIN ser_job_event_lookup ser
  ON a.date = ser.date
  AND a.org_id = ser.org_id
  AND a.device_id = ser.device_id
  AND a.event_ms = ser.event_ms
LEFT OUTER JOIN org_categories o
  ON a.org_id = o.org_id
