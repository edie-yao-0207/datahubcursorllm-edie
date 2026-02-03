WITH customer_operations_snapshot AS (
  SELECT
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.num_vg_shipped, eu.num_vg_shipped) AS num_vg_shipped,
    IFNULL(us.num_cm_shipped, eu.num_cm_shipped) AS num_cm_shipped,
    IFNULL(us.num_ag_shipped, eu.num_ag_shipped) AS num_ag_shipped,
    IFNULL(us.num_ig_shipped, eu.num_ig_shipped) AS num_ig_shipped,
    IFNULL(us.num_vg_purchased, eu.num_vg_purchased) AS num_vg_purchased,
    IFNULL(us.num_cm_purchased, eu.num_cm_purchased) AS num_cm_purchased,
    IFNULL(us.num_ag_purchased, eu.num_ag_purchased) AS num_ag_purchased,
    IFNULL(us.num_ig_purchased, eu.num_ig_purchased) AS num_ig_purchased,
    IFNULL(us.num_sg_shipped, eu.num_sg_shipped) AS num_sg_shipped,
    IFNULL(us.num_sc_shipped, eu.num_sc_shipped) AS num_sc_shipped,
    IFNULL(us.num_em_shipped, eu.num_em_shipped) AS num_em_shipped,
    IFNULL(us.num_sg_purchased, eu.num_sg_purchased) AS num_sg_purchased,
    IFNULL(us.num_sc_purchased, eu.num_sc_purchased) AS num_sc_purchased,
    IFNULL(us.num_em_purchased, eu.num_em_purchased) AS num_em_purchased
  FROM customer360.customer_operations_snapshot us
  FULL JOIN customer360_from_eu_west_1.customer_operations_snapshot eu ON
    us.sam_number = eu.sam_number
),

org_operations_snapshot AS (
  SELECT
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.num_vg_activated, 0) + IFNULL(eu.num_vg_activated, 0) AS num_vg_activated,
    IFNULL(us.num_cm_activated, 0) + IFNULL(eu.num_cm_activated, 0) AS num_cm_activated,
    IFNULL(us.num_ag_activated, 0) + IFNULL(eu.num_ag_activated, 0) AS num_ag_activated,
    IFNULL(us.num_ig_activated, 0) + IFNULL(eu.num_ig_activated, 0) AS num_ig_activated,
    IFNULL(us.num_vg_active, 0) + IFNULL(eu.num_vg_active, 0) AS num_vg_active,
    IFNULL(us.num_cm_active, 0) + IFNULL(eu.num_cm_active, 0) AS num_cm_active,
    IFNULL(us.num_ag_active, 0) + IFNULL(eu.num_ag_active, 0) AS num_ag_active,
    IFNULL(us.num_ig_active, 0) + IFNULL(eu.num_ig_active, 0) AS num_ig_active,
    IFNULL(us.num_passenger_vehicles, 0) + IFNULL(eu.num_passenger_vehicles, 0) AS num_passenger_vehicles,
    IFNULL(us.num_non_passenger_vehicles, 0) + IFNULL(eu.num_non_passenger_vehicles, 0) AS num_non_passenger_vehicles,
    IFNULL(us.num_sg_activated, 0) + IFNULL(eu.num_sg_activated, 0) AS num_sg_activated,
    IFNULL(us.num_sc_activated, 0) + IFNULL(eu.num_sc_activated, 0) AS num_sc_activated,
    IFNULL(us.num_em_activated, 0) + IFNULL(eu.num_em_activated, 0) AS num_em_activated,
    IFNULL(us.num_sg_active, 0) + IFNULL(eu.num_sg_active, 0) AS num_sg_active,
    IFNULL(us.num_sc_active, 0) + IFNULL(eu.num_sc_active, 0) AS num_sc_active,
    IFNULL(us.num_em_active, 0) + IFNULL(eu.num_em_active, 0) AS num_em_active
  FROM customer360.org_operations_snapshot us
  FULL JOIN customer360_from_eu_west_1.org_operations_snapshot eu ON
    us.org_id = eu.org_id
),

customer_metadata AS (
  SELECT 
    IFNULL(us.org_id, eu.org_id) AS org_id,
    IFNULL(us.sam_number, eu.sam_number) AS sam_number,
    IFNULL(us.sfdc_id, eu.sfdc_id) AS sfdc_id,
    IFNULL(us.name, eu.name) AS name,
    IFNULL(us.industry, eu.industry) AS industry,
    IFNULL(us.ae_id, eu.ae_id) AS ae_id,
    IFNULL(us.ae_name, eu.ae_name) AS ae_name,
    IFNULL(us.ae_email, eu.ae_email) AS ae_email,
    IFNULL(us.first_purchase_date, eu.first_purchase_date) AS first_purchase_date,
    IFNULL(us.status, eu.status) AS status,
    IFNULL(us.segment, eu.segment) AS segment,
    IFNULL(us.region, eu.region) AS region,
    IFNULL(us.billingcity, eu.billingcity) AS billingcity,
    IFNULL(us.billingpostalcode, eu.billingpostalcode) AS billingpostalcode,
    IFNULL(us.billingcountry, eu.billingcountry) AS billingcountry,
    IFNULL(us.billingcountrycode, eu.billingcountrycode) AS billingcountrycode,
    IFNULL(us.date_first_trial_shipped, eu.date_first_trial_shipped) AS date_first_trial_shipped,
    IFNULL(us.last_modified_date, eu.last_modified_date) AS last_modified_date,
    IFNULL(us.lifetime_acv, eu.lifetime_acv) AS lifetime_acv,
    IFNULL(us.delinquent, eu.delinquent) AS delinquent,
    IFNULL(us.org_size, eu.org_size) AS org_size,
    IFNULL(us.org_type, eu.org_type) AS org_type,
    IFNULL(us.implementation_consultant, eu.implementation_consultant) AS implementation_consultant,
    IFNULL(us.implementation_consultant_name, eu.implementation_consultant_name) AS implementation_consultant_name,
    IFNULL(us.csm, eu.csm) AS csm,
    IFNULL(us.csm_name, eu.csm_name) AS csm_name,
    IFNULL(us.csm_tier, eu.csm_tier) AS csm_tier
  FROM customer360.customer_metadata us
  FULL JOIN customer360_from_eu_west_1.customer_metadata eu ON
    us.org_id = eu.org_id
)

SELECT
  cm.org_id,
  cm.sam_number,
  cm.sfdc_id,
  cm.name,
  im.industry,
  cm.ae_id,
  cm.ae_name,
  cm.ae_email,
  cm.first_purchase_date,
  cm.status,
  cm.segment,
  cm.region,
  cm.billingcity,
  cm.billingpostalcode,
  cm.billingcountry,
  cm.billingcountrycode,
  cm.date_first_trial_shipped,
  cm.last_modified_date,
  cm.lifetime_acv,
  cm.delinquent,
  cm.org_size,
  cm.org_type,
  cm.implementation_consultant,
  cm.implementation_consultant_name,
  cm.csm,
  cm.csm_name,
  cm.csm_tier,
  cos.num_vg_shipped,
  cos.num_cm_shipped,
  cos.num_ag_shipped,
  cos.num_ig_shipped,
  cos.num_vg_purchased,
  cos.num_cm_purchased,
  cos.num_ag_purchased,
  cos.num_ig_purchased,
  oos.num_vg_activated,
  oos.num_cm_activated,
  oos.num_ag_activated,
  oos.num_ig_activated,
  oos.num_vg_active,
  oos.num_cm_active,
  oos.num_ag_active,
  oos.num_ig_active,
  oos.num_passenger_vehicles,
  oos.num_non_passenger_vehicles,
  css.assets AS assets_open,
  css.backend_infrastructure AS backend_infrastructure_open,
  css.connected_admin AS connected_admin_open,
  css.connected_driver AS connected_driver_open,
  css.dashcam AS dashcam_open,
  css.eurofleet AS eurofleet_open,
  css.fleet_app_ecosystem AS fleet_app_ecosystem_open,
  css.`fleet_management_-_other` AS `fleet_management_-_other_open`,
  css.hardware AS hardware_open,
  css.industrial AS industrial_open,
  css.multicam AS multicam_open,
  css.`non-product_related` AS `non-product_related_open`,
  css.platform AS platform_open,
  css.safety AS safety_open,
  css.telematics AS telematics_open,
  css.unknown AS unknown_open,
  cpus.total_dashboard_users,
  cpus.gamification_enabled,
  cpus.forward_collision_warning_enabled,
  cpus.distracted_driving_detection_enabled,
  cpus.following_distance_enabled,
  cpus.adas_all_features_allowed,
  cpus.safety_event_auto_triage_enabled,
  cpus.voice_coaching_enabled,
  cpus.canada_hos_enabled,
  cpus.safety_score_configured,
  cpus.eco_driving_score_enabled,
  cpus.safety_score_ranking_enabled,
  cos.num_sg_shipped,
  cos.num_sc_shipped,
  cos.num_em_shipped,
  cos.num_sg_purchased,
  cos.num_sc_purchased,
  cos.num_em_purchased,
  oos.num_sg_activated,
  oos.num_sc_activated,
  oos.num_em_activated,
  oos.num_sg_active,
  oos.num_sc_active,
  oos.num_em_active
FROM customer_metadata cm
LEFT JOIN definitions.industry_mapping im ON 
  cm.industry = im.sfdc_industry
LEFT JOIN customer_operations_snapshot cos ON
  cm.sam_number = cos.sam_number
LEFT JOIN org_operations_snapshot oos ON
  cm.org_id = oos.org_id
LEFT JOIN customer360.customer_support_snapshot css ON
  cm.sam_number = css.sam_number
LEFT JOIN customer360.org_platform_usage_snapshot cpus ON
  cm.org_id = cpus.org_id
WHERE
    cm.sam_number IS NOT NULL AND 
    cm.org_id IS NOT NULL
