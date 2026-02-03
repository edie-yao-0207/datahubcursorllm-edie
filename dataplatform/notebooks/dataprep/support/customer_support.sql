SET spark.databricks.delta.schema.autoMerge.enabled = TRUE;

CREATE OR REPLACE TEMP VIEW zendesk_tickets
USING bigquery
OPTIONS (
  table  'zd.support_tickets'
);

CREATE OR REPLACE TEMP VIEW zendesk_orgs
USING bigquery
OPTIONS (
  table  'zd.organizations_latest'
);

CREATE OR REPLACE TEMP VIEW ticket_data
USING bigquery
OPTIONS (
  table  'zd.tickets_latest'
);

CREATE OR REPLACE TEMP VIEW sam_orgs AS (
    SELECT
        osf.org_id,
        sf.sam_number,
        o.name as org_name
    FROM clouddb.org_sfdc_accounts osf
    LEFT JOIN clouddb.sfdc_accounts sf ON osf.sfdc_account_id = sf.id
    LEFT JOIN clouddb.organizations o on osf.org_id = o.id
);

CREATE OR REPLACE TEMP VIEW support_data AS (
    select 
        Ticket_ID as ticket_id, 
        sam_orgs.sam_number, 
        sam_orgs.org_id, 
        sam_orgs.org_name, 
        Date_Ticket_Created as created_date, 
        Product_Group as product_area,
        Product_Team as product_team,
        Case_Reason as case_reason,
        Ticket_Status as ticket_status
    from zendesk_tickets
    join ticket_data on ticket_data.id = ticket_id
    left join zendesk_orgs on ticket_data.organization_id = zendesk_orgs.id
    left join sam_orgs on zendesk_orgs.organization_fields.sam_number = sam_orgs.sam_number
);

CREATE TABLE IF NOT EXISTS dataprep.customer_support USING DELTA PARTITIONED BY (created_date) AS (
    SELECT *
    FROM support_data
);

INSERT OVERWRITE TABLE dataprep.customer_support (
    SELECT *
    FROM support_data
);
