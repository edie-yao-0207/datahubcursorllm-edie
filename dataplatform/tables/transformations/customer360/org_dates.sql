WITH org_first_heartbeat AS (
    SELECT
        orgs.id AS org_id,
        MIN(first_heartbeat_date) AS first_heartbeat_date
    FROM dataprep.device_heartbeats_extended
    JOIN clouddb.organizations orgs ON id = org_id
    WHERE internal_type != 1
    GROUP BY orgs.id
),

org_metadata_heartbeat AS (
    SELECT
        heartbeats.org_id, 
        sam_number,
        MIN(first_heartbeat_date) AS first_heartbeat_date
    FROM org_first_heartbeat heartbeats
    JOIN dataprep.customer_metadata metadata ON metadata.org_id = heartbeats.org_id
    GROUP BY
        heartbeats.org_id, 
        sam_number

),
org_metadata_dates AS (
    SELECT 
        *,
        EXPLODE(SEQUENCE(TO_DATE(first_heartbeat_date), CURRENT_DATE())) AS date
    FROM org_metadata_heartbeat
)

SELECT 
    *
FROM org_metadata_dates
WHERE date >= ${start_date}
AND date < ${end_date}
