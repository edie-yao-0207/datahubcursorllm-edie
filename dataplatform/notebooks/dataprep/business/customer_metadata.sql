-- Databricks notebook source

-- MAGIC %python
-- MAGIC import boto3
-- MAGIC
-- MAGIC def get_region() -> str:
-- MAGIC     """
-- MAGIC     Get EC2 region from AWS instance metadata.
-- MAGIC     """
-- MAGIC     session = boto3.session.Session()
-- MAGIC     return session.region_name
-- MAGIC
-- MAGIC def get_edw_catalog() -> str:
-- MAGIC     """
-- MAGIC     Returns the appropriate EDW catalog name to use based on region.
-- MAGIC     The EU/CA EDW tables are shared via a delta share so they have a different catalog name.
-- MAGIC     """
-- MAGIC     region = get_region()
-- MAGIC     if region == "us-west-2":
-- MAGIC         return "edw"
-- MAGIC     elif region in ("eu-west-1", "ca-central-1"):
-- MAGIC         return "edw_delta_share"
-- MAGIC     else:
-- MAGIC         raise RuntimeError(f"Invalid region {region}")
-- MAGIC
-- MAGIC # Set the EDW catalog name as a Spark configuration variable
-- MAGIC spark.conf.set("samsara.edw_catalog_to_use", get_edw_catalog())

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;

CREATE OR REPLACE TEMP VIEW salesforce_accounts_all AS (
  SELECT
    account_id AS id,
    account_name AS name,
    sam_number_undecorated AS sam_number_undecorated__c,
    industry AS industry,
    owner_id AS ownerid,
    first_purchase_date AS first_purchase_date__c,
    status AS status__c,
    segment AS segment__c,
    global_geography AS global_geography__c,
    billing_city AS billingcity,
    billing_postal_code AS billingpostalcode,
    billing_country AS billingcountry,
    billing_country_code AS billingcountrycode,
    first_trial_ship_date AS first_trial_ship_date__c,
    last_modified_date AS lastmodifieddate,
    currency_iso_code AS currencyisocode,
    account_lifetime_acv AS account_lifetime_acv__c,
    delinquent AS delinquent__c,
    organization_size AS organization_size__c,
    account_record_type AS record_type__c,
    csm AS csm__c,
    csm_health_rating AS csm_health_rating__c,
    csm_health_notes AS csm_health_notes__c,
    csm_health_trend AS csm_health_trend__c,
    sentiment_root_cause AS sentiment_root_cause__c,
    sic AS sic__c,
    cs_tier AS cs_tier__c,
    has_trial AS has_trial__c,
    total_open_opp_expected_revenue AS total_open_opp_expected_revenue__c,
    free_trial_kit_sent_date AS free_trial_kit_sent_date__c,
    customer_arr AS customer_arr__c,
    billing_first_name AS billing_first_name__c,
    billing_last_name AS billing_last_name__c,
    billing_email AS billing_email__c,
    account_size_segment AS account_size_segment__c,
    sub_industry AS sub_industry__c,
    billing_state_code AS billingstatecode,
    has_active_contract AS has_active_contract__c,
    support_escalation AS support_escalation__c,
    account_currently_escalated AS account_currently_escalated__c,
    high_churn_risk AS high_churn_risk__c,
    type AS type,
    se_assignment AS se_assignment__c,
    cs_poc_email_address AS cs_poc_email_address__c,
    total_expected_revenue AS total_expected_revenue__c,
    cloud_dashboard_link AS cloud_dashboard_link__c,
    days_post_first_purchase AS days_post_first_purchase__c,
    lifetime_purchases_currency AS lifetime_purchases_currency__c
  FROM
    ${samsara.edw_catalog_to_use}.salesforce_sterling.account
);

CREATE OR REPLACE TEMP VIEW customer_metadata AS (
  WITH revenue_opps AS (
    SELECT
      account_id as accountid,
      COUNT(1) AS revenue_opp_count
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity
    WHERE type = 'Revenue Opportunity' AND stage_name = 'Closed Won'
    GROUP BY accountid
  ),
  promo_opps AS (
    SELECT
      account_id as accountid,
      COUNT(1) AS promo_opp_count
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity
    WHERE type = 'Promo Opportunity' AND stage_name = 'Closed Won'
    GROUP BY accountid
  ),
  free_trial_opps AS (
    SELECT
      account_id as accountid,
      COUNT(1) as free_trial_opp_count
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity
    WHERE type = 'Free Trial Opportunity' AND stage_name = 'Closed Won'
    GROUP BY accountid
  ),
  exchange_and_refund_opps AS (
    SELECT
      account_id as accountid,
      COUNT(1) AS exchange_and_refund_opp_count
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity
    WHERE type = 'Warranty Exchange' OR type='Exchange' OR type = 'Remorse Refund'
    GROUP BY accountid
  ),
  beta_program_opps AS (
    SELECT
      account_id as accountid,
      COUNT(1) as beta_program_opp_count
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity
    WHERE promo_reason = 'Beta Program'
    GROUP BY accountid
  ),
  opportunity_counts AS (
    SELECT
      t1.accountid AS sfdc_id_long,
      t1.revenue_opp_count,
      t2.promo_opp_count,
      t3.free_trial_opp_count,
      t4.exchange_and_refund_opp_count,
      t5.beta_program_opp_count
    FROM revenue_opps t1
    LEFT JOIN promo_opps t2 ON
      t1.accountid = t2.accountid
    LEFT JOIN free_trial_opps t3 ON
      t2.accountid = t3.accountid
    LEFT JOIN exchange_and_refund_opps t4 ON
      t3.accountid = t4.accountid
    LEFT JOIN beta_program_opps t5 ON
      t4.accountid = t5.accountid
  ),
  feature_assignment AS (
    SELECT
      account_id AS sfdc_id_long,
      COUNT(1) AS num_opp_feature_assignments
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity t1
    JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.opportunity_feature_assignment t2 ON
      t1.opportunity_id = t2.opportunity
    GROUP BY t1.account_id
  ),
  release_type AS (
    SELECT DISTINCT
      id,
      release_type_enum AS release_type
    FROM default.clouddb.organizations
  ),
  customer_feedback AS (
    SELECT
      account_id AS sfdc_id_long,
      count(1) AS num_submitted_feedback
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.`case`
    WHERE reason = "Feedback"
    GROUP BY account_id
  ),
  campaign_members AS (
    SELECT
      account_id,
      COUNT(1) as number_of_campaign_members
    FROM ${samsara.edw_catalog_to_use}.salesforce_sterling.campaign_member t1
    LEFT JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.campaign t2 ON t1.campaign_id = t2.id
    WHERE t2.campaign_name_without_iteration LIKE "%Beta Testing%" AND
      t1.status IN ("Shipped", "Installed", "Completed Feedback")
    GROUP BY account_id
  ), ranked_currencies AS (
    SELECT
        currency_name AS name,
        currency_symbol AS symbol,
        exchange_rate,
        date_effective,
        ROW_NUMBER() OVER (PARTITION BY currency_symbol ORDER BY date_effective DESC) AS row_num
    FROM
        ${samsara.edw_catalog_to_use}.silver.dim_currency
    WHERE
        base_currency_symbol = 'USD'
), currency_exchange_rates AS (
  SELECT
      name,
      symbol,
      exchange_rate
  FROM
      ranked_currencies
  WHERE
      row_num = 1
)

  SELECT
      SUBSTRING(accounts.id, 1,15) AS sfdc_id,
      osf.org_id,
      accounts.name,
      sam_number_undecorated__c AS sam_number,
      accounts.industry,
      ownerid AS ae_id,
      users.name AS ae_name,
      users.email AS ae_email,
      to_date(first_purchase_date__c) AS first_purchase_date,
      status__c AS status,
      segment__c AS segment,
      global_geography__c AS region,
      billingcity,
      billingpostalcode,
      billingcountry,
      billingcountrycode,
      to_date(first_trial_ship_date__c) AS date_first_trial_shipped,
      to_date(accounts.lastmodifieddate) AS last_modified_date,
      CASE WHEN accounts.currencyisocode NOT LIKE '%USD%' THEN account_lifetime_acv__c * er.exchange_rate ELSE account_lifetime_acv__c END AS lifetime_acv,
      delinquent__c AS delinquent,
      organization_size__c AS org_size,
      record_type__c AS org_type,
      accounts.csm__c as implementation_consultant,
      ic.name AS implementation_consultant_name,
      csm_health_rating__c AS csm_rating,
      csm_health_notes__c AS csm_notes,
      csm_health_trend__c AS csm_trend,
      sentiment_root_cause__c AS csm_sentiment_cause,
      accounts.sic__c AS csm,
      csm.name AS csm_name,
      accounts.cs_tier__c AS csm_tier,
      accounts.has_trial__c as has_trial,
      accounts.total_open_opp_expected_revenue__c AS total_open_opp_expected_revenue,
      accounts.free_trial_kit_sent_date__c AS free_trial_kit_sent_date,
      accounts.customer_arr__c AS customer_arr,
      accounts.billing_first_name__c AS billing_first_name,
      accounts.billing_last_name__c AS billing_last_name,
      accounts.billing_email__c AS billing_email,
      SUBSTRING(accounts.id, 1,18) AS sfdc_id_long,
      accounts.account_size_segment__c AS size_segment,
      accounts.sub_industry__c AS sub_industry,
      accounts.billingstatecode,
      accounts.has_active_contract__c AS has_active_contract,
      accounts.support_escalation__c AS support_escalation,
      accounts.account_currently_escalated__c AS current_escalated,
      accounts.high_churn_risk__c AS high_churn_risk,
      accounts.type AS type,
      accounts.se_assignment__c AS se_assignment,
      accounts.cs_poc_email_address__c AS CS_POC_email_address,
      accounts.total_expected_revenue__c AS expected_ACV_revenue,
      accounts.cloud_dashboard_link__c AS cloud_dashboard_link,
      accounts.days_post_first_purchase__c AS days_since_first_purchase,
      oc.revenue_opp_count,
      oc.promo_opp_count,
      oc.free_trial_opp_count,
      oc.exchange_and_refund_opp_count,
      oc.beta_program_opp_count,
      fa.num_opp_feature_assignments,
      rt.release_type,
      cf.num_submitted_feedback,
      cm.number_of_campaign_members,
      ic.email AS ic_email,
      csm.email AS csm_email,
      se.name AS se_name,
      accounts.lifetime_purchases_currency__c as lifetime_purchases
    FROM salesforce_accounts_all accounts
    INNER JOIN default.clouddb.sfdc_accounts sf ON
      SUBSTRING(accounts.id, 1, 15) = SUBSTRING(sf.sfdc_account_id, 1, 15)
    INNER JOIN default.clouddb.org_sfdc_accounts osf ON
      osf.sfdc_account_id = sf.id
    LEFT JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.`user` users ON
      users.user_id = ownerid
    LEFT JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.`user` ic ON
      ic.user_id = accounts.csm__c
    LEFT JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.`user` csm ON
      csm.user_id = accounts.sic__c
    LEFT JOIN ${samsara.edw_catalog_to_use}.salesforce_sterling.`user` se ON
      se.user_id = accounts.se_assignment__c
    LEFT JOIN currency_exchange_rates er ON
      accounts.currencyisocode = er.symbol
    LEFT JOIN opportunity_counts oc ON
      accounts.id = oc.sfdc_id_long
    LEFT JOIN feature_assignment fa ON
      accounts.id = fa.sfdc_id_long
    LEFT JOIN release_type rt ON
      osf.org_id = rt.id
    LEFT JOIN customer_feedback cf ON
      accounts.id = cf.sfdc_id_long
    LEFT JOIN campaign_members cm ON
      SUBSTRING(accounts.id, 1,15) = cm.account_id
);

CREATE TABLE IF NOT EXISTS dataprep.customer_metadata USING DELTA (
  SELECT *
  FROM customer_metadata
);

INSERT OVERWRITE TABLE dataprep.customer_metadata (
  SELECT *
  FROM customer_metadata
);
