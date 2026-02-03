-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Year In Review
-- MAGIC This notebook generates the data for the Year In Review report
-- MAGIC
-- MAGIC The final metrics are uploaded as a CSV to S3 where they are consumed by the Platform Reports team in a Go service, to then
-- MAGIC show on the cloud dashboard.
-- MAGIC
-- MAGIC **Cluster requirements**. This must run on a cluster with:
-- MAGIC - DBR 12.0 or above (see `INSERT INTO` ... `REPLACE WHERE` in `Combine all metrics together` section)
-- MAGIC - Permission to write to the `year_review_metrics` database & the `samsara-{eu}-year-review-metrics` s3 bucket
-- MAGIC   - The dataplatform or dataengineering clusters have this permission

-- COMMAND ----------

-- DBTITLE 1,Create Date Parameters
CREATE WIDGET TEXT startDate DEFAULT '2024-01-01';
CREATE WIDGET TEXT endDate DEFAULT '2024-12-31';
CREATE WIDGET TEXT year DEFAULT '2024';

CREATE WIDGET TEXT activeDevicesOnOrAfterDateForOrgEligiblityCriteria DEFAULT '2024-08-01';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Select & print out the parameters; some weird Databricks bug happens where it says
-- MAGIC the widget is not defined, so adding this helps uncover and re-run that early so then you can
-- MAGIC just hit "run all" on the notebook

-- COMMAND ----------

SELECT
  getArgument("startDate"),
  getArgument("endDate"),
  getArgument("activeDevicesOnOrAfterDateForOrgEligiblityCriteria"),
  getArgument("year")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(dbutils.widgets.get("startDate"))
-- MAGIC print(dbutils.widgets.get("endDate"))
-- MAGIC print(dbutils.widgets.get("endDate"))
-- MAGIC print(dbutils.widgets.get("year"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Helper functions used throughout the notebook

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from datetime import datetime
-- MAGIC
-- MAGIC import boto3
-- MAGIC
-- MAGIC def get_region() -> str:
-- MAGIC     """
-- MAGIC     Get EC2 region from AWS instance metadata.
-- MAGIC
-- MAGIC     Reference: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
-- MAGIC     """
-- MAGIC     session = boto3.session.Session()
-- MAGIC     return session.region_name
-- MAGIC
-- MAGIC def get_year(date_str):
-- MAGIC   dt = datetime.strptime(date_str, '%Y-%m-%d')
-- MAGIC   return dt.year
-- MAGIC
-- MAGIC def get_s3_bucket_to_upload_data_to() -> str:
-- MAGIC   s3_bucket_us = f"s3://samsara-year-review-metrics"
-- MAGIC   s3_bucket_eu = f"s3://samsara-eu-year-review-metrics"
-- MAGIC
-- MAGIC   region = get_region()
-- MAGIC   if region == "us-west-2":
-- MAGIC     return s3_bucket_us
-- MAGIC   elif region == "eu-west-1":
-- MAGIC     return s3_bucket_eu
-- MAGIC   else:
-- MAGIC     raise RuntimeError(f"Invalid region {region}")
-- MAGIC
-- MAGIC def upload_query_to_s3_as_csv(query, s3_full_path):
-- MAGIC   df = spark.sql(query)
-- MAGIC   df.coalesce(1).write.format("csv").mode('overwrite').option("header", True).save(s3_full_path)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Eligibility Orgs/Customers
-- MAGIC Which orgs are we generating metrics for?
-- MAGIC
-- MAGIC Orgs who have had both:
-- MAGIC - Greater than or equal to 3 active VGs since Aug 2024 and `endDate` parameter above
-- MAGIC - Total trip distance across all VGs > 1k miles

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.active_vehicles_per_org_since_activeDevicesOnOrAfterDateForOrgEligiblityCriteria as (
  select
    year(getArgument("endDate")) as year,
    getArgument("activeDevicesOnOrAfterDateForOrgEligiblityCriteria") as start_date,
    getArgument("endDate") as end_date,
    lifetime_device_activity.org_id,
    COUNT(DISTINCT lifetime_device_activity.device_id) as num_unique_active_vehicles
  FROM datamodel_core.lifetime_device_activity
  INNER JOIN datamodel_core.dim_devices
    ON dim_devices.device_id = lifetime_device_activity.device_id
    AND dim_devices.org_id = lifetime_device_activity.org_id
    AND dim_devices.date = getArgument("endDate")
  WHERE
    -- this is a date partitioned table, so only care about the most recent partition
    lifetime_device_activity.date = getArgument("endDate")

    -- This filters for only devices that were active b/w start and activeDevicesOnOrAfterDateForOrgEligiblityCriteria
    AND lifetime_device_activity.latest_date between getArgument("activeDevicesOnOrAfterDateForOrgEligiblityCriteria") and getArgument("endDate")
    AND (
      dim_devices.device_type like 'VG%'
      OR dim_devices.product_id = 58 -- this is product_type "OEMV", which was used in 2022. TODO: that until we update dim_devices to include this as a vg, it won't use trips to determien activeness. Then we can also just remove this line
    )
  GROUP BY
    lifetime_device_activity.org_id
);

SELECT * FROM year_review_metrics.active_vehicles_per_org_since_activeDevicesOnOrAfterDateForOrgEligiblityCriteria LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.trip_distance_metrics_vgs_only as (
  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    fct_trips.org_id,
    sum(distance_miles) as total_trip_distance_miles
  FROM datamodel_telematics.fct_trips
  INNER JOIN datamodel_core.dim_devices
    ON dim_devices.device_id = fct_trips.device_id
    AND dim_devices.org_id = fct_trips.org_id
    AND dim_devices.date = getArgument("endDate")
  WHERE
    fct_trips.date between getArgument("startDate") AND getArgument("endDate")

    -- For eligible org criteria for miles driven, we only care about VGs
    AND (
      dim_devices.device_type like 'VG%'
      OR dim_devices.product_id = 58 -- this is product_type "OEMV", which was used in 2022. TODO: that until we update dim_devices to include this as a vg, we must keep this part in. Then we can just remove this line
    )
    AND fct_trips.trip_type = 'location_based'
  GROUP BY fct_trips.org_id, start_date, end_date
);

SELECT * FROM year_review_metrics.trip_distance_metrics_vgs_only LIMIT 10

-- COMMAND ----------

CREATE or REPLACE TABLE year_review_metrics.eligible_orgs as (

  WITH dim_orgs AS (
    SELECT * FROM datamodel_core.dim_organizations WHERE date = getArgument("endDate")
  ),

  cloud_dashboard_org_to_sam_mapping AS (
    SELECT
      organizations.id as org_id,
      sfdc_accounts.sam_number as sam_number
    FROM clouddb.organizations
    LEFT OUTER JOIN clouddb.org_sfdc_accounts ON org_sfdc_accounts.org_id = organizations.id
    LEFT OUTER JOIN clouddb.sfdc_accounts ON sfdc_accounts.id = org_sfdc_accounts.sfdc_account_id
  )

  SELECT
    year(getArgument("endDate")) as year,
    getArgument("endDate") as date,
    trip_metrics.org_id,


    dim_orgs.org_name,
    -- Knowing whether the org is internal or external is helpful, in case we want to filter internal orgs out
    CASE WHEN dim_orgs.internal_type = 0 then 'customer' else 'internal' END as org_type,

    dim_orgs.sam_number AS sam_number_from_datamodel,
    cloud_dashboard_org_to_sam_mapping.sam_number AS sam_number_from_cloud_dashboard,
    CASE
        WHEN dim_orgs.sam_number = cloud_dashboard_org_to_sam_mapping.sam_number THEN true
        ELSE false
      END as sam_numbers_equal,
    dim_orgs.account_industry as industry,

    CASE
      WHEN cloud_db_organizations.release_type_enum = 0 then 'PHASE_1'
      WHEN cloud_db_organizations.release_type_enum = 1 then 'PHASE_2'
      WHEN cloud_db_organizations.release_type_enum = 2 then 'EARLY_ADOPTER'
      ELSE 'unknown'
      END AS release_type,

    active_vehicles_per_org.num_unique_active_vehicles,
    trip_metrics.total_trip_distance_miles
  FROM year_review_metrics.trip_distance_metrics_vgs_only as trip_metrics
  INNER JOIN year_review_metrics.active_vehicles_per_org_since_activeDevicesOnOrAfterDateForOrgEligiblityCriteria as active_vehicles_per_org USING (org_id, year)
  LEFT OUTER JOIN dim_orgs USING (org_id)
  LEFT OUTER JOIN cloud_dashboard_org_to_sam_mapping USING (org_id)
  LEFT OUTER JOIN clouddb.organizations as cloud_db_organizations ON trip_metrics.org_id = cloud_db_organizations.id
  WHERE
    active_vehicles_per_org.num_unique_active_vehicles >= 3
    AND trip_metrics.total_trip_distance_miles >= 1000
);

SELECT * FROM year_review_metrics.eligible_orgs LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Eligible users associated with eligible orgs
-- MAGIC Generate a table of all users who have the necessary permissions in these eligible orgs.
-- MAGIC
-- MAGIC Alongside each user, put what org they are in.
-- MAGIC
-- MAGIC If the user is in multiple orgs, choose the org with the most number of active vehicles

-- COMMAND ----------

CREATE or REPLACE TABLE year_review_metrics.eligible_users_amongst_eligible_orgs as (

  WITH eligible_users_with_orgs_ranked_by_num_unique_active_vehicles AS (
    SELECT
      DISTINCT
      users.id as user_id,
      users.email_lower as user_email,
      users.name as user_name,

      eligible_orgs.org_id,
      eligible_orgs.org_name,
      eligible_orgs.sam_number_from_datamodel,
      eligible_orgs.sam_number_from_cloud_dashboard,
      eligible_orgs.sam_numbers_equal,
      eligible_orgs.release_type,
      ROW_NUMBER() OVER (
        PARTITION BY users_organizations.user_id
        ORDER BY eligible_orgs.num_unique_active_vehicles DESC
      ) AS num_unique_active_vehicles_rank
    FROM clouddb.users
    LEFT OUTER JOIN clouddb.users_organizations ON users_organizations.user_id = users.id
    LEFT OUTER JOIN clouddb.custom_roles ON custom_roles.uuid = users_organizations.custom_role_uuid
    -- INNER JOIN ensures we only get users who are part of eligible orgs
    INNER JOIN year_review_metrics.eligible_orgs ON eligible_orgs.org_id = users_organizations.organization_id
    LATERAL VIEW explode(custom_roles.permissions.permissions) AS exploded_permissions
    LATERAL VIEW explode(custom_roles.permissions.permission_sets) AS exploded_permission_sets
    WHERE
      (exploded_permissions.id = 'essentials.reports'
      or exploded_permission_sets.id = 'essentials') -- people with correct permissions
      AND eligible_orgs.org_type = 'customer' -- we only care about this list for customer orgs
      AND users.email_lower NOT LIKE '%@samsara.com' -- we do not need to send comms to any Samsara users
      AND users_organizations.tag_id IS NULL -- no tag-level admins
  )

  SELECT
    year(getArgument("endDate")) as year,
    user_id,
    user_email,
    user_name,
    org_id,
    org_name,
    release_type,
    sam_number_from_datamodel,
    sam_number_from_cloud_dashboard,
    sam_numbers_equal
  FROM eligible_users_with_orgs_ranked_by_num_unique_active_vehicles
  WHERE num_unique_active_vehicles_rank = 1

);

SELECT * FROM year_review_metrics.eligible_users_amongst_eligible_orgs LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Upload Eligible Orgs & User Lists to S3
-- MAGIC So we can download and then share with stakeholders
-- MAGIC
-- MAGIC Don't use download from Databricks notebook directly since results can be truncated

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Upload all eligible orgs
-- MAGIC Including what release_type is important, as marketing needs to know which are Phase 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC s3_bucket = get_s3_bucket_to_upload_data_to()
-- MAGIC current_year = get_year(dbutils.widgets.get("endDate"))
-- MAGIC s3_full_path = f"{s3_bucket}/year_review/non_report_data/{current_year}/all_eligible_orgs_{get_region()}"
-- MAGIC
-- MAGIC query = f"""
-- MAGIC   SELECT
-- MAGIC     org_id
-- MAGIC     org_type,
-- MAGIC     sam_number_from_datamodel,
-- MAGIC     sam_number_from_cloud_dashboard,
-- MAGIC     sam_numbers_equal,
-- MAGIC     industry,
-- MAGIC     release_type
-- MAGIC   FROM year_review_metrics.eligible_orgs
-- MAGIC   WHERE year ='{current_year}'
-- MAGIC """
-- MAGIC
-- MAGIC upload_query_to_s3_as_csv(query, s3_full_path)
-- MAGIC print(f"Uploaded results to {s3_full_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Upload eligible users amongst orgs

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC s3_bucket = get_s3_bucket_to_upload_data_to()
-- MAGIC current_year = get_year(dbutils.widgets.get("endDate"))
-- MAGIC s3_full_path = f"{s3_bucket}/year_review/non_report_data/{current_year}/full_admins_for_non_phase_2_orgs_only_{get_region()}"
-- MAGIC
-- MAGIC query = f"""
-- MAGIC   SELECT
-- MAGIC     user_id,
-- MAGIC     user_email,
-- MAGIC     user_name,
-- MAGIC     org_id,
-- MAGIC     org_name,
-- MAGIC     release_type,
-- MAGIC     sam_number_from_datamodel,
-- MAGIC     sam_number_from_cloud_dashboard,
-- MAGIC     sam_numbers_equal
-- MAGIC   FROM year_review_metrics.eligible_users_amongst_eligible_orgs
-- MAGIC   WHERE year ='{current_year}'
-- MAGIC """
-- MAGIC
-- MAGIC upload_query_to_s3_as_csv(query, s3_full_path)
-- MAGIC print(f"Uploaded results to {s3_full_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Trips metrics
-- MAGIC All metrics related to the "Trips" section of year in review.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Trip Distance
-- MAGIC
-- MAGIC Note: unlike the eligible orgs critera above, this calculates over all device types, not just VGS
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Trips metrics
-- MAGIC All metrics related to the "Trips" section of year in review.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Trip Distance
-- MAGIC
-- MAGIC Note: unlike the eligible orgs critera above, this calculates over all device types, not just VGS
-- MAGIC
-- MAGIC Note: This means in a way this could be seen as "double-counting" if we are not clear on what the metric means
-- MAGIC       For example if both a VG and an AG go on a trip together, there are 2 rows in this table for that trip and hence we add up the mileage and hours from both
-- MAGIC       As of 2024, we do not have a reliable way to determine if a VG and AG were associated for a trip; hence, we cannot filter out the AG mileage if we see the VG and AG were on the trip together.
-- MAGIC       And note there are many AG-only trips, so we can't just ignore all AG trips, or we really under-count for some orgs.

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.trip_distance_metrics as (
  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    org_id,
    sum(distance_miles) as total_trip_distance_miles,
    sum(duration_mins) / 60 as total_trip_duration_hours,
    count(1) as total_num_trips,
    max(distance_miles) as longest_trip_distance_miles
  FROM datamodel_telematics.fct_trips
  WHERE
    date >= getArgument("startDate")
    AND date <= getArgument("endDate")

    -- Only calculate for orgs in the eligible orgs list
    AND org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)
    AND trip_type = 'location_based'
  GROUP BY org_id, start_date, end_date
);

SELECT * FROM year_review_metrics.trip_distance_metrics LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Number of active vehicles per org
-- MAGIC This is used to then calculate average miles per vehicle further down

-- COMMAND ----------

-- This is the same query as active_vehicles_per_org_since_activeDevicesOnOrAfterDateForOrgEligiblityCriteria,
-- however, it uses startDate and endDate, instead of activeDevicesOnOrAfterDateForOrgEligiblityCriteria
CREATE OR REPLACE TABLE year_review_metrics.active_devices_per_org as (
  select
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    lifetime_device_activity.org_id,
    COUNT(DISTINCT lifetime_device_activity.device_id) as num_unique_active_vgs_and_ags
  FROM datamodel_core.lifetime_device_activity
  INNER JOIN datamodel_core.dim_devices
    ON dim_devices.device_id = lifetime_device_activity.device_id
    AND dim_devices.org_id = lifetime_device_activity.org_id
    AND dim_devices.date = getArgument("endDate")
  WHERE
    -- this is a date partitioned table, so only care about the most recent partition
    lifetime_device_activity.date = getArgument("endDate")

    -- This filters for only devices that were active b/w start and end date
    AND lifetime_device_activity.latest_date between getArgument("startDate") and getArgument("endDate")
    AND (
      -- We want to track active VGs or AGs, since the mileage metrics (from trips) has both VGs and AGS

      dim_devices.device_type like 'VG%'
      OR dim_devices.product_id = 58 -- this is product_type "OEMV", which was used in 2022. TODO: that until we update dim_devices to include this as a vg, it won't use trips to determien activeness. Then we can also just remove this line

      --
      OR dim_devices.device_type like 'AG%'
    )

    -- Only calculate for orgs in the eligible orgs list
    AND lifetime_device_activity.org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)
  GROUP BY
    lifetime_device_activity.org_id
);

SELECT * FROM year_review_metrics.active_devices_per_org LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Avg Miles Per Asset
-- MAGIC The miles per asset is calculated as (Num VG miles + Num AG miles) / (Num VGs + Num AGs)
-- MAGIC This is b/c our trips metrics are both VG and AG miles, so to get a reasonable and non misleading avg, we need to include both

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.mileage_metrics as (
  SELECT
    trip_metrics.year,
    trip_metrics.start_date,
    trip_metrics.end_date,
    trip_metrics.org_id,

    trip_metrics.total_trip_distance_miles / active_devices_per_org.num_unique_active_vgs_and_ags as avg_miles_per_asset
  FROM year_review_metrics.trip_distance_metrics as trip_metrics
  LEFT JOIN year_review_metrics.active_devices_per_org USING (org_id, year)
);

SELECT * FROM year_review_metrics.mileage_metrics LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Dashboard Usage Stats

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Total Dashboard Page Views
-- MAGIC Note this is all pages, not just reports

-- COMMAND ----------

  CREATE OR REPLACE TABLE year_review_metrics.dashboard_usage_metrics as (
    SELECT
      year(getArgument("endDate")) as year,
      getArgument("startDate") as start_date,
      getArgument("endDate") as end_date,
      org_id,
      COUNT(1) as total_page_view_count
    FROM datamodel_platform_silver.stg_cloud_routes
    WHERE
      date between getArgument("startDate") and getArgument("endDate")
      -- exclude page loads from Samsara users
      and userEmail not like "%@samsara.com"

      -- Only calculate for orgs in the eligible orgs list
      AND org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)
    GROUP BY year, start_date, end_date, org_id
  );

  SELECT * FROM year_review_metrics.dashboard_usage_metrics LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Most Frequent Visited Reports
-- MAGIC
-- MAGIC This is just reports under `/o/:org_idfleet/reports/` prefix

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.cloud_dashboard_report_page_loads as (
  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    stg_cloud_routes.*,
    route_config.path as ResourceName
  FROM datamodel_platform_silver.stg_cloud_routes stg_cloud_routes
  INNER JOIN perf_infra.route_config on route_config.routeName = stg_cloud_routes.routename
  WHERE
    stg_cloud_routes.date between getArgument("startDate") and getArgument("endDate")
    -- exclude page loads from Samsara users
    AND stg_cloud_routes.userEmail not like "%@samsara.com"
    -- We only care about "Reports" which all have this prefix
    AND route_config.path not like "/o/:org_id/fleet/reports/index"
    AND route_config.path like "/o/:org_id/fleet/reports/%"

    -- Only calculate for orgs in the eligible orgs list
    AND org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)
);

SELECT * FROM year_review_metrics.cloud_dashboard_report_page_loads LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Big long SQL query to map urls to Report Names that we show in the Year In Review

CREATE OR REPLACE TABLE year_review_metrics.cloud_dashboard_report_page_loads_with_report_name as (
  SELECT
    cloud_dashboard_report_page_loads.*,
    case
      when (
        ResourceName = "/o/:org_id/fleet/reports/activity"
        or ResourceName = "/o/:org_id/fleet/reports/activity/report"
      ) then "activity"
      when (
        ResourceName = "/o/:org_id/fleet/reports/asset/billing"
        or ResourceName = "/o/:org_id/fleet/reports/asset/billing/site"
      ) then "billing"
      when ResourceName = "/o/:org_id/fleet/reports/asset/dormancy" then "asset_dormancy"
      when ResourceName = "/o/:org_id/fleet/reports/asset/schedule" then "asset_schedule"
      when ResourceName = "/o/:org_id/fleet/reports/camera_health" then "camera_health"
      when (
        ResourceName = "/o/:org_id/fleet/reports/cameras"
        or ResourceName = "/o/:org_id/fleet/reports/cameras/:deviceId/trips"
        or ResourceName = "/o/:org_id/fleet/reports/cameras/visual_review"
        or ResourceName = "/o/:org_id/fleet/reports/multicam"
        or ResourceName = "/o/:org_id/fleet/reports/multicam/trip/:deviceId"
      ) then "cameras"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/coaching"
        or ResourceName = "/o/:org_id/fleet/reports/coaching/driver/:driverId/events/:eventIdentifiers"
        or ResourceName = "/o/:org_id/fleet/reports/coaching/manager/:safetyManagerId"
        or ResourceName = "/o/:org_id/fleet/reports/coaching/coaching_sessions"
        or ResourceName = "/o/:org_id/fleet/reports/coaching_summary/:uuid"
        or ResourceName = "/o/:org_id/fleet/reports/safety/coach_reports/coach/:safetyManagerId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/coach_reports/coaching_effectiveness/:safetyManagerId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/coach_reports/coaching_timeliness/:safetyManagerId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/coaches/effectiveness"
        or ResourceName = "/o/:org_id/fleet/reports/safety/coaches/timeliness"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/event_resolution"
        or ResourceName = "/o/:org_id/fleet/reports/coaching/coaching_session/:uuid" -- added in 2023
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/nudges" -- added in 2023
      ) then "coaching"
      when (
        ResourceName = "/o/:org_id/fleet/reports/colocation"
        or ResourceName = "/o/:org_id/fleet/reports/colocation/:uuid"
        or ResourceName = "/o/:org_id/fleet/reports/colocation/:uuid/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/colocation/create"
      ) then "co-location"
      when ResourceName = "/o/:org_id/fleet/reports/compliance_dashboard" then "compliance_dashboard"
      when ResourceName = "/o/:org_id/fleet/reports/safety/crash" then "crash"
      when (
        ResourceName = "/o/:org_id/fleet/reports/custom"
        or ResourceName = "/o/:org_id/fleet/reports/custom/:configUuid"
      ) then "custom"
      when (
        ResourceName = "/o/:org_id/fleet/reports/dashcam"
        or ResourceName = "/o/:org_id/fleet/reports/dashcam/:deviceId"
        or ResourceName = "/o/:org_id/fleet/reports/dashcam/:deviceId/trip/:startMs"
      ) then "dashcam"
      when (
        ResourceName = "/o/:org_id/fleet/reports/asset/detention"
        or ResourceName = "/o/:org_id/fleet/reports/asset/detention/site"
      ) then "detention"
      when (
        ResourceName = "/o/:org_id/fleet/reports/driver_assignment"
        or ResourceName = "/o/:org_id/fleet/reports/driver_assignment/driver/:driverId/training_images"
      ) then "driver_assignment"
      when (
        ResourceName = "/o/:org_id/fleet/reports/documents"
        or ResourceName = "/o/:org_id/fleet/reports/documents/:driverId/:driverCreatedAtMs"
        or ResourceName = "/o/:org_id/fleet/reports/documents/:driverId/:driverCreatedAtMs/edit"
        or ResourceName = "/o/:org_id/fleet/reports/documents/type/:uuid/edit"
        or ResourceName = "/o/:org_id/fleet/reports/documents/type/create"
      ) then "driver_documents"
      when ResourceName = "/o/:org_id/fleet/reports/driver_efficiency" then "driver_efficiency"
      when (
        ResourceName = "/o/:org_id/fleet/reports/hos_audit"
        or ResourceName = "/o/:org_id/fleet/reports/hos_audit/report"
        or ResourceName = "/o/:org_id/fleet/reports/hos_transfer"
      ) then "driver_hours_of_service_audit"
      when ResourceName = "/o/:org_id/fleet/reports/driver_qualifications/dashboard" then "driver_qualifications"
      when (
        ResourceName = "/o/:org_id/fleet/reports/drivers_hours"
        or ResourceName = "/o/:org_id/fleet/reports/drivers_hours/:driverId"
      ) then "driver_hours"
      when ResourceName = "/o/:org_id/fleet/reports/hos/status" then "duty_status_summary"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/digio"
        or ResourceName = "/o/:org_id/fleet/reports/digio/report"
      ) then "equipment_report"
      when (
        ResourceName = "/o/:org_id/fleet/reports/ev_charging"
        or ResourceName = "/o/:org_id/fleet/reports/ev_charging/location"
        or ResourceName = "/o/:org_id/fleet/reports/ev_charging/vehicle"
        or ResourceName = "/o/:org_id/fleet/reports/ev_efficiency"
      ) then "ev_charging"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/travel_expenses"
        or ResourceName = "/o/:org_id/fleet/reports/travel_expenses/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/expenses"
        or ResourceName = "/o/:org_id/fleet/reports/expenses/:driverId"
      ) then "expense"
      when ResourceName = "/o/:org_id/fleet/reports/fleet_benchmarks" then "fleet_benchmarks"
      when ResourceName = "/o/:org_id/fleet/reports/fleet_electrification" then "fleet_electrification"
      when (
        ResourceName = "/o/:org_id/fleet/reports/fuel"
        or ResourceName = "/o/:org_id/fleet/reports/fuel_energy"
      ) then "fuel_&_energy"
      when (
        ResourceName = "/o/:org_id/fleet/reports/fuel_purchases"
        or ResourceName = "/o/:org_id/fleet/reports/fuel_purchases/purchase/:uuid"
      ) then "fuel_purchases"
      when ResourceName = "/o/:org_id/fleet/reports/gateway_health" then "gateway_health"
      when ResourceName = "/o/:org_id/fleet/reports/safety/harsh_event/:harshEventLabel" then "harsh_driving"
      when ResourceName = "/o/:org_id/fleet/reports/asset/historic_diagnostic" then "historic_diagnostic"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/hos/driver"
        or ResourceName = "/o/:org_id/fleet/reports/hos/driver/driver/:driver_id"
        or ResourceName = "/o/:org_id/fleet/reports/hos/driver_classic"
        or ResourceName = "/o/:org_id/fleet/reports/hos/driver_v2"
        or ResourceName = "/o/:org_id/fleet/reports/hos/log_edit_report"
        or ResourceName = "/o/:org_id/fleet/reports/hos/driver/:driver_id"
      ) then "hours_of_service"
      when (
        ResourceName = "/o/:org_id/fleet/reports/hos/violations"
        or ResourceName = "/o/:org_id/fleet/reports/hos/violations_v2"
      ) then "hours_of_service_violations"
      when ResourceName = "/o/:org_id/fleet/reports/ifta" then "IFTA"
      when (
        ResourceName = "/o/:org_id/fleet/reports/infringement"
        or ResourceName = "/o/:org_id/fleet/reports/infringement/:driverId"
      ) then "infringement"
      when (
        ResourceName = "/o/:org_id/fleet/reports/asset/inventory"
        or ResourceName = "/o/:org_id/fleet/reports/asset/inventory/:siteAddressId/show"
        or ResourceName = "/o/:org_id/fleet/reports/asset/inventory/search"
        or ResourceName = "/o/:org_id/fleet/reports/asset/inventory/search/report"
      ) then "inventory"
      when (
        ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/address/:addressId"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/2/vehicle/:vehicleId"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/routes/planned_vs_actual/by_assignees/vehicle/:vehicleId"
      ) then "planned_vs_actual"
      when (
        ResourceName = "/o/:org_id/fleet/reports/privacy_sessions"
        or ResourceName = "/o/:org_id/fleet/reports/privacy_sessions/:deviceId"
      ) then "privacy_sessions"
      when (
        ResourceName = "/o/:org_id/fleet/reports/route_runs"
        or ResourceName = "/o/:org_id/fleet/reports/route_runs/:recurringRouteId"
      ) then "recurring_routes"
      when (
        ResourceName = "/o/:org_id/fleet/reports/asset/reefer"
        or ResourceName = "/o/:org_id/fleet/reports/asset/reefer/:deviceId"
      ) then "reefer"
      when (
        ResourceName = "/o/:org_id/fleet/reports/edu/rides"
        or ResourceName = "/o/:org_id/fleet/reports/edu/rides/routes"
        or ResourceName = "/o/:org_id/fleet/reports/edu/rides/scans"
        or ResourceName = "/o/:org_id/fleet/reports/edu/rides/students"
        or ResourceName = "/o/:org_id/fleet/reports/edu/rides/vehicles"
      ) then "rides"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/safety"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/event_resolution"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/summary"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/table"
        or ResourceName = "/o/:org_id/fleet/reports/safety/dashboard/vehicle/:vehicleId"
      ) then "safety_dashboard"
      when (
        ResourceName = "/o/:org_id/fleet/reports/safety_inbox"
        or ResourceName = "/o/:org_id/fleet/reports/safety_inbox/speeding" -- added in 2023
      ) then "safety_inbox"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/collision_risk"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/crash"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/distracted_driving"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/harsh_events"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/policy_violations"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/speeding"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/speeding/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/speeding/vehicle/:vehicleId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/risk_factors/traffic_signs"
        or ResourceName = "/o/:org_id/fleet/reports/safety/tag"
        or ResourceName = "/o/:org_id/fleet/reports/safety/vehicle/:vehicleId"
        or ResourceName = "/o/:org_id/fleet/reports/safety/vehicle/:vehicleId/incident/:atMs"
        or ResourceName = "/o/:org_id/fleet/reports/safety/speeding/devices/:device_id/trip/:trip_id"
        or ResourceName = "/o/:org_id/fleet/reports/safety/total_harsh_events"
      ) then "safety"
      when ResourceName = "/o/:org_id/fleet/reports/sensor_health" then "sensor_health"
      when ResourceName = "/o/:org_id/fleet/reports/vehicle_timesheet" then "start/stop_report"
      when ResourceName = "/o/:org_id/fleet/reports/tachograph_explorer" then "tachograph_explorer"
      when (
        ResourceName = "/o/:org_id/fleet/reports/remote_tachograph"
        or ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/driver/:cardNumber"
        or ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/driver/:cardNumber/analysis/:timestamp"
        or ResourceName = "/o/:org_id/fleet/reports/remote_tachograph/vehicle/:deviceId"
      ) then "tachograph_file_downloads"
      when (
        ResourceName = "/o/:org_id/fleet/reports/site"
        or ResourceName = "/o/:org_id/fleet/reports/site/:addressId/show"
        or ResourceName = "/o/:org_id/fleet/reports/site/address"
        or ResourceName = "/o/:org_id/fleet/reports/site/unknown"
        or ResourceName = "/o/:org_id/fleet/reports/site/vehicle"
        or ResourceName = "/o/:org_id/fleet/reports/site/vehicle/:deviceId/show"
        or ResourceName = "/o/:org_id/fleet/reports/site/vehicle_v2"
      ) then "time_on-site"
      when (
        -- updated in 2022
        ResourceName = "/o/:org_id/fleet/reports/trips"
        or ResourceName = "/o/:org_id/fleet/reports/trips/asset"
        or ResourceName = "/o/:org_id/fleet/reports/trips/asset/:deviceId"
        or ResourceName = "/o/:org_id/fleet/reports/trips/driver"
        or ResourceName = "/o/:org_id/fleet/reports/trips/driver/:driverId"
      ) then "trip_history"
      when (
        ResourceName = "/o/:org_id/fleet/reports/unassigned_tachograph_driving"
        or ResourceName = "/o/:org_id/fleet/reports/unassigned_tachograph_driving/:deviceId"
      ) then "unassigned_driving"
      when (
        ResourceName = "/o/:org_id/fleet/reports/hos/unassigned"
        or ResourceName = "/o/:org_id/fleet/reports/hos/unassigned/:vehicleId"
        or ResourceName = "/o/:org_id/fleet/reports/hos/unassigned_v2"
      ) then "unassigned_HOS"
      when ResourceName = "/o/:org_id/fleet/reports/asset/utilization" then "utilization"
      when (
        ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments"
        or ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments/driver/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/vehicle_assignments/vehicle/:vehicleId"
      ) then "vehicle_assignments"
      when (
        ResourceName = "/o/:org_id/fleet/reports/video_retrieval"
        or ResourceName = "/o/:org_id/fleet/reports/video_retrieval/v2"
        or ResourceName = "/o/:org_id/fleet/reports/video_library" -- added in 2023
      ) then "video_retrieval"
      when (
        ResourceName = "/o/:org_id/fleet/reports/data_usage"
      ) then "data_usage"
      when (ResourceName = "/o/:org_id/fleet/reports/idling") then "idling"
      when (
        ResourceName = "/o/:org_id/fleet/reports/material_usage"
      ) then "material_usage"
      when (
        ResourceName = "/o/:org_id/fleet/reports/mileage"
      ) then "jurisdiction_mileage"
      when (
        ResourceName = "/o/:org_id/fleet/reports/tachograph_health"
      ) then "tachograph_health"
      when (
        ResourceName = "/o/:org_id/fleet/reports/timesheet"
        or ResourceName = "/o/:org_id/fleet/reports/timesheet/:driverId"
        or ResourceName = "/o/:org_id/fleet/reports/timesheet/edits"
        or ResourceName = "/o/:org_id/fleet/reports/timesheet/edits/:driverId"
      ) then "timesheet"
      when (
        ResourceName = "/o/:org_id/fleet/reports/forms/submissions"
        or ResourceName = "/o/:org_id/fleet/reports/forms/issues"
        or ResourceName = "/o/:org_id/fleet/reports/forms/templates"
        or ResourceName = "/o/:org_id/fleet/reports/forms/submissions/:submissionUuid"
        or ResourceName = "/o/:org_id/fleet/reports/forms/issues/:submissionUuid"
        or ResourceName = "/o/:org_id/fleet/reports/forms/templates/:templateUuid"
        or ResourceName = "/o/:org_id/fleet/reports/forms/templates/create"
        or ResourceName = "/o/:org_id/fleet/reports/forms/config"
      ) then "forms"
      when ResourceName = "/o/:org_id/fleet/reports/sustainability" then "sustainability_report"
      when ResourceName = "/o/:org_id/fleet/reports/ev_suitability" then "ev_suitability"
      when ResourceName = "/o/:org_id/fleet/reports/device_policy_report" then "device_policy"
      when ResourceName = "/o/:org_id/fleet/reports/accessories_health" then "accessories_health"
      when ResourceName = "/o/:org_id/fleet/reports/charge_control" then "ev_charging"
      when ResourceName = "/o/:org_id/fleet/reports/connectivity_health" then "connectivity_health"
      when ResourceName = "/o/:org_id/fleet/reports/immobilization" then "immobilization_report"
      when ResourceName = "/o/:org_id/fleet/reports/dashboard" then "dashboard"
      else null
    end as report_name
from
  year_review_metrics.cloud_dashboard_report_page_loads
);

SELECT * FROM year_review_metrics.cloud_dashboard_report_page_loads_with_report_name LIMIT 10;

-- COMMAND ----------

/*
  This query determines the top 5 reports that each org visited, along with how many times they visited each of those reports
  A report is determined by the mapping of urls to reports, which happens in the cell above.
*/

CREATE OR REPLACE TABLE year_review_metrics.dashboard_report_usage_metrics as (

  WITH report_ranks_by_org as (
    SELECT
      org_id,
      report_name,
      count(1) as num_report_page_loads,
      row_number() over (partition by org_id order by count(1) desc) as report_rank
    FROM year_review_metrics.cloud_dashboard_report_page_loads_with_report_name
    WHERE report_name is not null
    GROUP BY org_id, report_name
  ),

  top_5_reports_by_org as (
    SELECT
      org_id,
      report_name,
      num_report_page_loads,
      report_rank
    FROM report_ranks_by_org
    WHERE report_rank <= 5
  ),

  top_5_reports_by_org_pivot as (
    select *
    from (
      select org_id, report_name, report_rank
      from top_5_reports_by_org
    ) t
    pivot (
      first(report_name)
      for report_rank in (1 as report_1 , 2 as report_2, 3 as report_3, 4 as report_4, 5 as report_5)
    )
    order by org_id
  ),

  top_5_reports_by_org_pivot_count_values as (
    select *
    from (
      select org_id, num_report_page_loads, report_rank
      from top_5_reports_by_org
    ) t
    pivot (
      first(num_report_page_loads)
      for report_rank in (1 as report_1_count , 2 as report_2_count, 3 as report_3_count, 4 as report_4_count, 5 as report_5_count)
    )
    order by org_id
  )


  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,

    top_5_reports_by_org_pivot.org_id,
    top_5_reports_by_org_pivot.report_1,
    top_5_reports_by_org_pivot.report_2,
    top_5_reports_by_org_pivot.report_3,
    top_5_reports_by_org_pivot.report_4,
    top_5_reports_by_org_pivot.report_5,

    top_5_reports_by_org_pivot_count_values.report_1_count,
    top_5_reports_by_org_pivot_count_values.report_2_count,
    top_5_reports_by_org_pivot_count_values.report_3_count,
    top_5_reports_by_org_pivot_count_values.report_4_count,
    top_5_reports_by_org_pivot_count_values.report_5_count
  FROM top_5_reports_by_org_pivot
  INNER JOIN top_5_reports_by_org_pivot_count_values USING (org_id)
);

SELECT* FROM year_review_metrics.dashboard_report_usage_metrics LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Safety metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Safety event & coaching metrics

-- COMMAND ----------

create or replace table year_review_metrics.safety_events as (
  select
    fct_safety_events.*
  FROM datamodel_safety.fct_safety_events

  -- raw safety event table needed for now in order to get the speed information (see where clause below)
  LEFT OUTER JOIN safetydb_shards.safety_events raw_safety_events_tbl USING (org_id, device_id, event_ms, date)

  WHERE
    fct_safety_events.date between getArgument("startDate") and getArgument("endDate")

    -- Only calculate for orgs in the eligible orgs list
    AND org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)

    -- Filter out events that during coaching they are dismissed, manually review dismissed, or auto-dismissed
    -- In other words, if the event was coached, we only want to include it if it has not been dismissed, manually review dismissed, or auto-dismissed
    AND (
      was_event_coached is false
      OR
      (was_event_coached is true
        AND NOT(array_contains(coaching_state_names_array, "Dismissed"))
        AND NOT(array_contains(coaching_state_names_array, "Manual Review Dismissed"))
        AND NOT(array_contains(coaching_state_names_array, "Auto-Dismissed"))
      )
    )

    -- Filter out safety events that did not happen on a trip
    AND COALESCE(fct_safety_events.trip_start_ms, 0) != 0

    -- Filter out events that happen under 5 mph unless
    -- they are haCrash or haRolledStopSign
    AND (
      detection_label IN ("haCrash", "haRolledStopSign", "haTileRollingStopSign", "haTileRollingRailroadCrossing", "haRollover") OR
      (
        raw_safety_events_tbl.detail_proto.start.speed_milliknots >= 4344.8812095 AND
        raw_safety_events_tbl.detail_proto.stop.speed_milliknots >= 4344.8812095
      )
    )

    -- Filter out safety events that are not released to customers
    -- 0 = UNSET, 1 = DARK_LAUNCHED
    -- 2 = CLOSED_BETA, 3 = OPEN_BETA, 4 = OPEN_BETA
    AND raw_safety_events_tbl.release_stage >= 2
);

SELECT * FROM year_review_metrics.safety_events LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.safety_event_metrics AS (

  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    org_id,

    -- event_id field should not be considered unique, rather the combo of org_id, device_id, event_ms
    -- Since we group by org_id, just distinct on latter 2
    COUNT(distinct device_id, event_ms) as num_events_total,

    SUM (CASE WHEN was_event_coached is true then 1 else 0 end) AS num_events_coached,
    -- "Reviewed" for the purpose of Year In Review is if it's been reviewed, recognized, or coached
    SUM (CASE WHEN array_contains(coaching_state_names_array, "Reviewed") OR  array_contains(coaching_state_names_array, "Recognized") or was_event_coached is true then 1 else 0 end) AS num_events_reviewed
  FROM
    year_review_metrics.safety_events
  WHERE
    date between getArgument("startDate") and getArgument("endDate")
  GROUP BY year, start_date, end_date, org_id
);

SELECT * FROM year_review_metrics.safety_event_metrics LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Safety video upload metrics
-- MAGIC Video uploads are defined as videos from one of 2 sources: 1) # of safety event video uploads and 2) # video retrievals

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Safety event video uploads

-- COMMAND ----------

-- A table of every video file uploaded
CREATE OR REPLACE TABLE year_review_metrics.uploaded_videos_with_durations as (

    SELECT
      org_id,
      value.proto_value.uploaded_file_set.event_id as event_id,
      -- this field is an array of integers that represent the durations of each video file in the event
      -- this adds those integers up
      aggregate(value.proto_value.uploaded_file_set.dashcam_file_infos.duration_ms,
                 cast(0 as bigint),
                 (acc, x) -> cast(acc as bigint) + IF(x IS NULL, cast(0 as bigint), cast(x as bigint))
                ) AS sum_of_duration_in_ms_of_videos_in_event
    FROM
      kinesisstats_history.osduploadedfileset
    where
      date between getArgument("startDate") and getArgument("endDate")
      and org_id in (select org_id from year_review_metrics.eligible_orgs)

      and value.proto_value.uploaded_file_set.event_id is not null
      -- Only care about files that have corresponding safety events
      and value.proto_value.uploaded_file_set.event_id IN (select event_id FROM year_review_metrics.safety_events)

      -- Must contain at least 1 video file
      and EXISTS(
        value.proto_value.uploaded_file_set.s3urls,
        url -> url like '%.mp4'
      )
);

select * from year_review_metrics.uploaded_videos_with_durations LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.safety_events_video_upload_metrics AS (
  SELECT
    org_id.
    COUNT(1) as num_safety_events_with_video_uploads.
    SUM(sum_of_duration_in_ms_of_videos_in_event / 1000 / 60) AS safety_event_video_uploads_duration_minutes
  FROM year_review_metrics.safety_events
  INNER JOIN year_review_metrics.uploaded_videos_with_durations USING (org_id, event_id)
  GROUP BY org_id
);

SELECT * FROM year_review_metrics.safety_events_video_upload_metrics LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Video retrieval requests

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.successful_video_retrievals AS (
  select
    organizations.id as org_id,
    historical_video_requests.device_id,
    (historical_video_requests.end_ms - historical_video_requests.start_ms) / 1000 / 60 as video_duration_minutes
  from
    clouddb.historical_video_requests as historical_video_requests
    INNER JOIN clouddb.groups on groups.id = historical_video_requests.group_id
    INNER JOIN clouddb.organizations ON organizations.id = groups.organization_id
    INNER JOIN clouddb.devices on historical_video_requests.device_id = devices.id AND organizations.id = devices.org_id
  WHERE
    historical_video_requests.date between getArgument("startDate") and getArgument("endDate")
    AND historical_video_requests.state = 2 -- these are successful ones
    and historical_video_requests.created_at_ms != 0

    -- Only calculate for orgs in the eligible orgs list
    AND organizations.id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)
);

SELECT * FROM year_review_metrics.successful_video_retrievals LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.video_retrieval_metrics AS (
  SELECT
    successful_video_retrievals.org_id,
    COUNT(1) as num_successful_video_retrievals,
    SUM(video_duration_minutes) AS successful_video_retrievals_minutes
  FROM year_review_metrics.successful_video_retrievals
  GROUP BY org_id
);

SELECT * FROM year_review_metrics.video_retrieval_metrics LIMIT 10;

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.safety_video_upload_metrics AS (
  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    org_id,

    -- Just video retrieval metrics
    video_retrieval_metrics.num_successful_video_retrievals,
    video_retrieval_metrics.successful_video_retrievals_minutes,

    -- Video retrieval + safety events added togetehr
    video_retrieval_metrics.num_successful_video_retrievals + safety_events_video_upload_metrics.num_safety_events_with_video_uploads AS total_num_video_uploads,
    video_retrieval_metrics.successful_video_retrievals_minutes + safety_events_video_upload_metrics.safety_event_video_uploads_duration_minutes AS total_video_uploads_duration_minutes
  FROM year_review_metrics.safety_events_video_upload_metrics
  FULL OUTER JOIN year_review_metrics.video_retrieval_metrics USING (org_id)
);

SELECT * FROM year_review_metrics.safety_video_upload_metrics LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Fuel metrics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Average MPG

-- COMMAND ----------

-- This has been updated to use the new MPGE calculation

CREATE OR REPLACE TABLE year_review_metrics.fuel_metrics AS (

  WITH dim_devices as (
    SELECT * FROM datamodel_core.dim_devices WHERE date = getArgument("endDate")
  ),

  -- Coalesce null values to 0, and convert to miles and gallons
  feer_data_values_coalesced as (
    SELECT
      org_id,
      object_id as device_id,
      date,
      object_type,
      COALESCE(distance_traveled_m, 0) as distance_traveled_m,
      COALESCE(electric_distance_traveled_m, 0) as electric_distance_traveled_m,
      COALESCE((fuel_consumed_ml + fuel_consumed_ml_as_gaseous_type), 0) as fuel_consumed_ml,
      COALESCE(energy_consumed_kwh, 0) as energy_consumed_kwh,
      COALESCE(gaseous_fuel_consumed_grams, 0) AS gaseous_fuel_consumed_grams
    FROM fuel_energy_efficiency_report.aggregated_vehicle_driver_rows_v4
  ),

  -- Emit a row for each device, and calculate that device's MPG
  mpg_by_device as (
    SELECT
      feer_data_values_coalesced.org_id,
      feer_data_values_coalesced.device_id,
      CASE
      WHEN SUM(fuel_consumed_ml) > 0
      and SUM(energy_consumed_kwh) > 0 AND SUM(electric_distance_traveled_m) > 0 THEN SUM(electric_distance_traveled_m) / SUM(distance_traveled_m) * 33705 / (
        SUM(energy_consumed_kwh) * 1000 / (
          SUM(electric_distance_traveled_m) * 0.0006213711922
        )
      ) + (
        1 - SUM(electric_distance_traveled_m) / SUM(distance_traveled_m)
      ) * (
        SUM(distance_traveled_m) - SUM(electric_distance_traveled_m)
      ) * 0.0006213711922 / SUM(fuel_consumed_ml * 0.000264172)
      WHEN SUM(fuel_consumed_ml) = 0
      AND SUM(energy_consumed_kwh) > 0 AND SUM(electric_distance_traveled_m) > 0 THEN 33705 / (
        SUM(energy_consumed_kwh) * 1000 / (
          SUM(electric_distance_traveled_m) * 0.0006213711922
        )
      )
      WHEN SUM(fuel_consumed_ml) > 0
      THEN SUM(distance_traveled_m) * 0.0006213711922 / SUM(fuel_consumed_ml * 0.000264172)
      ELSE 0
    END weighted_mpge,
    CASE
      WHEN SUM(gaseous_fuel_consumed_grams) > 0 THEN SUM(distance_traveled_m * 0.0006213711922) / SUM(
        gaseous_fuel_consumed_grams * 1.4 * 0.000264172
      )
      ELSE 0
    END gaseous_mpg,
    SUM(fuel_consumed_ml) * 0.000264172 as total_fuel_consumed_gallons,
    SUM(distance_traveled_m) * 0.0006213711922 as total_distance_traveled_miles,
    SUM(electric_distance_traveled_m) as total_electric_distance_traveled_m,
    SUM(energy_consumed_kwh) as total_energy_consumed_kwh,
    abs(fuel_consumed_ml) + abs(energy_consumed_kwh) > 0 as has_fuel_consumed
    FROM feer_data_values_coalesced
    INNER JOIN dim_devices USING (org_id, device_id)
    WHERE
      feer_data_values_coalesced.date between getArgument("startDate") and getArgument("endDate")

      -- Only care about VGs for Average MPG
      AND dim_devices.device_type like 'VG%'

      -- Filter out any rows where no fuel consumed, to avoid divide by 0
      AND feer_data_values_coalesced.fuel_consumed_ml >= 0

      AND feer_data_values_coalesced.object_type = 1 -- object type 1 = vehicle; only agg over vehicles (not drivers)

      -- Only calculate for orgs in the eligible orgs list
      AND feer_data_values_coalesced.org_id IN (SELECT org_id FROM year_review_metrics.eligible_orgs)

    GROUP BY feer_data_values_coalesced.org_id, feer_data_values_coalesced.device_id, feer_data_values_coalesced.fuel_consumed_ml, feer_data_values_coalesced.energy_consumed_kwh
  ),
  total_calc as (
    select org_id,
    sum(
      IF(
        has_fuel_consumed
        and total_distance_traveled_miles > 0,
        weighted_mpge * total_distance_traveled_miles,
        NULL
      )
    ) / sum(
      IF(
        has_fuel_consumed
        and total_distance_traveled_miles > 0,
        total_distance_traveled_miles,
        NULL
      )
    ) as weighted_mpg
    from mpg_by_device
    group by org_id
  )

  -- Now calculate AVG mpg across an org by averaging across all the devices
  SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    org_id,
    avg(weighted_mpg) as average_mpg
  FROM total_calc
  GROUP BY org_id

);

SELECT * FROM year_review_metrics.fuel_metrics ORDER by average_mpg DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # API metrics
-- MAGIC Total number of API calls and tokens used by orgs

-- COMMAND ----------

SET spark.databricks.queryWatchdog.maxQueryTasks=2000000; -- Need to set this, othwerwise get org.apache.spark.SparkException error. "Launched too many tasks in a single stage"

CREATE OR REPLACE TABLE year_review_metrics.api_requests_by_org AS (
SELECT
    year(getArgument("endDate")) as year,
    getArgument("startDate") as start_date,
    getArgument("endDate") as end_date,
    org_id,
    sum(num_requests) as api_requests_count,
    count(distinct api_token_id) as num_unique_api_tokens
  FROM product_analytics.fct_api_usage
  WHERE
    date between getArgument("startDate") and getArgument("endDate")

    -- Intentionally include all orgs b/c below we want to do a percentile rank
    -- across all orgs. So unlike most queries in this noteboo, we are explicitly NOT adding a filter for eligible orgs only
  GROUP BY org_id
);

SELECT * FROM year_review_metrics.api_requests_by_org LIMIT 10

-- COMMAND ----------

CREATE OR REPLACE TABLE year_review_metrics.api_usage_metrics AS (
    SELECT
      year(getArgument("endDate")) as year,
      getArgument("startDate") as start_date,
      getArgument("endDate") as end_date,
      org_id,

      api_requests_count,
      num_unique_api_tokens,

      -- Where this org ranks compared to all other orgs in terms of # of api requests
      percent_rank() over (order by api_requests_count) * 100 as api_request_count_percentile
    FROM year_review_metrics.api_requests_by_org
    WHERE api_requests_count > 0
);

SELECT * FROM year_review_metrics.api_usage_metrics ORDER BY api_request_count_percentile DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Combine all metrics together
-- MAGIC Combine all metrics into a single table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS year_review_metrics.final_output (
    year STRING,
    start_date STRING,
    end_date STRING,

    org_id BIGINT,

    -- Trips metrics
    total_trip_distance_miles DOUBLE,
    total_trip_duration_hours DOUBLE,
    total_num_trips BIGINT,
    longest_trip_distance_miles DOUBLE,
    avg_miles_per_asset DOUBLE,

    -- Dashboard page route load metrics
    total_page_view_count BIGINT,

    report_1 STRING,
    report_2 STRING,
    report_3 STRING,
    report_4 STRING,
    report_5 STRING,

    report_1_count BIGINT,
    report_2_count BIGINT,
    report_3_count BIGINT,

    -- Safety metrics
    num_events_total BIGINT,
    num_events_coached BIGINT,
    num_events_reviewed BIGINT,

    total_num_video_uploads BIGINT,
    total_video_uploads_duration_minutes DOUBLE,
    successful_video_requests BIGINT,
    successful_video_duration_minutes DOUBLE,

     -- Fuel metrics
    average_mpg DOUBLE,

    -- API metrics
    api_token_count BIGINT,
    api_total_count BIGINT,
    api_total_count_percentile DOUBLE
);

-- COMMAND ----------

-- To get `INSERT INTO` to work as expected with `REPLACE WHERE` over a non-partitioned table,
-- this must run on DBR 12.0 or above. See https://docs.databricks.com/en/delta/selective-overwrite.html#arbitrary-selective-overwrite-with-replacewhere
INSERT INTO TABLE  year_review_metrics.final_output
REPLACE WHERE `year` = '2024' -- TODO: change this each year; we can't get `getArgument("someVar")` to work here, seems to be a bug
SELECT
   getArgument("year") AS `year`,
   getArgument("startDate") as start_date,
   getArgument("endDate") as end_date,

  -- org information
  eligible_orgs.org_id,

  -- Trips metrics
  trip_distance_metrics.total_trip_distance_miles,
  trip_distance_metrics.total_trip_duration_hours,
  trip_distance_metrics.total_num_trips,
  trip_distance_metrics.longest_trip_distance_miles,

  mileage_metrics.avg_miles_per_asset,

  -- Dashboard page route load metrics
  dashboard_usage_metrics.total_page_view_count,

  dashboard_report_usage_metrics.report_1,
  dashboard_report_usage_metrics.report_2,
  dashboard_report_usage_metrics.report_3,
  dashboard_report_usage_metrics.report_4,
  dashboard_report_usage_metrics.report_5,

  dashboard_report_usage_metrics.report_1_count,
  dashboard_report_usage_metrics.report_2_count,
  dashboard_report_usage_metrics.report_3_count,

  -- Safety metrics
  safety_event_metrics.num_events_total,
  safety_event_metrics.num_events_coached,
  safety_event_metrics.num_events_reviewed,

  safety_video_upload_metrics.total_num_video_uploads,
  safety_video_upload_metrics.total_video_uploads_duration_minutes,

  safety_video_upload_metrics.num_successful_video_retrievals AS successful_video_requests,
  safety_video_upload_metrics.successful_video_retrievals_minutes AS successful_video_duration_minutes,

  fuel_metrics.average_mpg,

  -- API metrics
  api_usage_metrics.num_unique_api_tokens as api_token_count,
  api_usage_metrics.api_requests_count as api_total_count,
  api_usage_metrics.api_request_count_percentile as api_total_count_percentile

FROM year_review_metrics.eligible_orgs
LEFT OUTER JOIN year_review_metrics.trip_distance_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.safety_event_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.dashboard_report_usage_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.dashboard_usage_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.api_usage_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.safety_video_upload_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.fuel_metrics USING (org_id, year)
LEFT OUTER JOIN year_review_metrics.mileage_metrics USING (org_id, year)

-- COMMAND ----------

SELECT * FROM year_review_metrics.final_output WHERE year = year(getArgument("endDate")) LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Write the metrics to S3 location as CSV

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC current_year = get_year(dbutils.widgets.get("endDate"))
-- MAGIC
-- MAGIC ## The order of the columns in this select statement
-- MAGIC # must match the order the Go code expects the CSV to be in
-- MAGIC
-- MAGIC final_query_to_generate_csv = f"""
-- MAGIC   select
-- MAGIC     org_id,
-- MAGIC
-- MAGIC     total_trip_distance_miles,
-- MAGIC     total_trip_duration_hours,
-- MAGIC     total_num_trips,
-- MAGIC     longest_trip_distance_miles,
-- MAGIC
-- MAGIC     average_mpg,
-- MAGIC
-- MAGIC     -- These address_ columns are legacy columns that are no longer in the report, but the CSV still expects it, otherwise parsing will fail.
-- MAGIC     0 as address_1,
-- MAGIC     0 as address_2,
-- MAGIC     0 as address_3,
-- MAGIC     0 as address_4,
-- MAGIC     0 as address_5,
-- MAGIC
-- MAGIC     num_events_total,
-- MAGIC     num_events_coached,
-- MAGIC     num_events_reviewed,
-- MAGIC
-- MAGIC     successful_video_requests,
-- MAGIC     successful_video_duration_minutes,
-- MAGIC
-- MAGIC     report_1,
-- MAGIC     report_2,
-- MAGIC     report_3,
-- MAGIC     report_1_count,
-- MAGIC     report_2_count,
-- MAGIC     report_3_count,
-- MAGIC
-- MAGIC     total_page_view_count,
-- MAGIC
-- MAGIC     api_token_count,
-- MAGIC     api_total_count,
-- MAGIC     api_total_count_percentile,
-- MAGIC
-- MAGIC     avg_miles_per_vehicle,
-- MAGIC
-- MAGIC     report_4,
-- MAGIC     report_5,
-- MAGIC
-- MAGIC     total_num_video_uploads,
-- MAGIC     total_video_uploads_duration_minutes,
-- MAGIC
-- MAGIC     -- safety_score is a legacy column that is no longer in the report, but the CSV still expects it, otherwise parsing will fail.
-- MAGIC     0 as safety_score
-- MAGIC
-- MAGIC   FROM year_review_metrics.final_output
-- MAGIC   WHERE year = '{current_year}'
-- MAGIC """
-- MAGIC print(final_query_to_generate_csv)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # This cell actually uploads the result of the query to S3
-- MAGIC
-- MAGIC s3_bucket = get_s3_bucket_to_upload_data_to()
-- MAGIC current_year = get_year(dbutils.widgets.get("endDate"))
-- MAGIC s3_full_path = f"{s3_bucket}/year_review/data_{current_year}_prod"
-- MAGIC
-- MAGIC upload_query_to_s3_as_csv(final_query_to_generate_csv, s3_full_path)
-- MAGIC print(f"Uploaded results to {s3_full_path}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Validation of data
-- MAGIC This is validation done in 2024. Adjust as needed for future years
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC def get_stats(column_name):
-- MAGIC   sql = f"""
-- MAGIC     WITH dim_orgs as (
-- MAGIC     select * FROM datamodel_core.dim_organizations where date = (select max(date) FROM datamodel_core.dim_organizations)
-- MAGIC   )
-- MAGIC
-- MAGIC   select
-- MAGIC     dim_orgs.account_cs_tier,
-- MAGIC     format_number(bround(min(final_output.{column_name}), 2), 0) as min_value,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.25), 2), 0) as `25th_percentile`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.5), 2), 0) as `median`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.75), 2), 0) as `75th_percentile`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.90), 2), 0) as `90th_percentile`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.95), 2), 0) as `95th_percentile`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.99), 2), 0) as `99th_percentile`,
-- MAGIC     format_number(bround(percentile(final_output.{column_name}, 0.999), 2), 0) as `99.9th_percentile`,
-- MAGIC     format_number(bround(max(final_output.{column_name}), 2), 0) as max_value
-- MAGIC   from
-- MAGIC     year_review_metrics.final_output
-- MAGIC   LEFT OUTER JOIN year_review_metrics.eligible_orgs USING (year, org_id)
-- MAGIC   LEFT OUTER JOIN dim_orgs USING (org_id)
-- MAGIC   where
-- MAGIC     year = '2024'
-- MAGIC     AND eligible_orgs.org_type = 'customer'
-- MAGIC   GROUP BY 1
-- MAGIC   ORDER BY percentile(final_output.{column_name}, 0.999) asc
-- MAGIC   """
-- MAGIC   df = spark.sql(sql)
-- MAGIC   return df

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_outliers(column_name, tier):
-- MAGIC   sql = f"""
-- MAGIC   WITH dim_orgs as (
-- MAGIC     select * FROM datamodel_core.dim_organizations where date = (select max(date) FROM datamodel_core.dim_organizations)
-- MAGIC   )
-- MAGIC
-- MAGIC select
-- MAGIC   org_id,
-- MAGIC   format_number(final_output.{column_name}, 0),
-- MAGIC   eligible_orgs.*
-- MAGIC from year_review_metrics.final_output
-- MAGIC left outer join year_review_metrics.eligible_orgs USING (year, org_id)
-- MAGIC LEFT OUTER JOIN dim_orgs USING (org_id)
-- MAGIC where
-- MAGIC   year = '2024'
-- MAGIC   AND eligible_orgs.org_type = 'customer'
-- MAGIC   AND dim_orgs.account_cs_tier = '{tier}'
-- MAGIC order by {column_name} desc
-- MAGIC limit 10
-- MAGIC   """
-- MAGIC   return spark.sql(sql)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_number_orgs_per_tier_past_threshold(column_name, threshold):
-- MAGIC   sql = f"""
-- MAGIC       WITH dim_orgs as (
-- MAGIC     select * FROM datamodel_core.dim_organizations where date = (select max(date) FROM datamodel_core.dim_organizations)
-- MAGIC   )
-- MAGIC
-- MAGIC select
-- MAGIC   dim_orgs.account_cs_tier,
-- MAGIC   count(distinct org_id)
-- MAGIC from year_review_metrics.final_output
-- MAGIC left outer join year_review_metrics.eligible_orgs USING (year, org_id)
-- MAGIC LEFT OUTER JOIN dim_orgs USING (org_id)
-- MAGIC where
-- MAGIC   year = '2024'
-- MAGIC   AND eligible_orgs.org_type = 'customer'
-- MAGIC   and  final_output.{column_name} > {threshold}
-- MAGIC group by 1
-- MAGIC   """
-- MAGIC   return spark.sql(sql)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def get_orgs_per_tier_past_threshold(column_name, threshold):
-- MAGIC   sql = f"""
-- MAGIC       WITH dim_orgs as (
-- MAGIC     select * FROM datamodel_core.dim_organizations where date = (select max(date) FROM datamodel_core.dim_organizations)
-- MAGIC   )
-- MAGIC
-- MAGIC select
-- MAGIC   eligible_orgs.org_id,
-- MAGIC   eligible_orgs.org_name,
-- MAGIC   eligible_orgs.sam_number_from_cloud_dashboard,
-- MAGIC   eligible_orgs.release_type,
-- MAGIC   eligible_orgs.num_unique_active_vehicles,
-- MAGIC   final_output.{column_name}
-- MAGIC from year_review_metrics.final_output
-- MAGIC left outer join year_review_metrics.eligible_orgs USING (year, org_id)
-- MAGIC LEFT OUTER JOIN dim_orgs USING (org_id)
-- MAGIC where
-- MAGIC   year = '2024'
-- MAGIC   AND eligible_orgs.org_type = 'customer'
-- MAGIC   and  final_output.{column_name} > {threshold}
-- MAGIC   """
-- MAGIC   return spark.sql(sql)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_trip_distance_miles"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_trip_duration_hours"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_num_trips"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("longest_trip_distance_miles"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("avg_miles_per_asset"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_page_view_count"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("num_events_total"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("num_events_coached"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("num_events_reviewed"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_num_video_uploads"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("total_video_uploads_duration_minutes"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Dig in further since it seems some Starter rows have really big numbers
-- MAGIC # Result: after clickign through some dashboards, we think this is expected as these orgs
-- MAGIC # have really long hyperlapse videos
-- MAGIC display(get_outliers("total_video_uploads_duration_minutes", "Starter"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_number_orgs_per_tier_past_threshold("total_video_uploads_duration_minutes", 400_000))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_orgs_per_tier_past_threshold("total_video_uploads_duration_minutes", 400_000))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("average_mpg"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Dig in further since it seems some Starter rows have really big numbers
-- MAGIC # Result: we ended up fixing the query to filter out EVs; now this shouldn't really show many outliers if run again
-- MAGIC display(get_outliers("average_mpg", "Starter"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_number_orgs_per_tier_past_threshold("average_mpg", 100))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_orgs_per_tier_past_threshold("average_mpg", 100))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("api_token_count"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("api_total_count"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(get_stats("api_total_count_percentile"))
