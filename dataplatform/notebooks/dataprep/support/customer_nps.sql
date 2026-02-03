CREATE OR REPLACE TEMP VIEW delighted_data
USING bigquery
OPTIONS (
  table  'delighted.responses_decorated_v2'
);

INSERT OVERWRITE TABLE dataprep.customer_nps (
    SELECT
        Timestamp AS timestamp,
        DATE(Timestamp) AS date,
        Delighted_ID as delighted_id,
        Delighted_Raw_Score AS raw_score,
        Delighted_NPS_Score AS nps_score,
        Delighted_Comment AS comment,
        Email AS email,
        SAM AS sam_number,
        Org_Id AS org_id,
        HOS_User AS hos_user,
        Delighted_Properties AS delighted_properties,
        Delighted_Role AS delighted_role,
        Locale AS locale,
        Delighted_Account_ID AS delighted_account_id,
        Delighted_Contact_ID AS delighted_contact_id,
        Theme AS theme,
        Bucket AS bucket,
        Subcategory AS subcategory,
        PG AS pg
    FROM delighted_data
);
