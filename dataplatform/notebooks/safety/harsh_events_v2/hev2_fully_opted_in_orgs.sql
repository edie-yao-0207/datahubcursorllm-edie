-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW self_served_orgs AS (
	SELECT org_id, ss.updated_at AS opt_in_date, 'self served' AS cohort from releasemanagementdb_shards.feature_package_self_serve ss
	INNER JOIN clouddb.organizations o ON o.id = ss.org_id
	AND o.internal_type = 0 -- customer orgs
	AND feature_package_uuid = upper("af02640685ae4549af78fbc5584db713")
	AND enabled = 1 -- is enabled
  AND scope = 1 -- org scope
);

-- closed beta group
CREATE OR REPLACE TEMP VIEW closed_beta_orgs AS (
	SELECT id AS org_id, TO_DATE('01-01-2022','dd-MM-yyyy') AS opt_in_date, 'closed beta' AS cohort
	FROM clouddb.organizations WHERE id in (
		29501,48139,37121,32986,26681,23103,26495,56863,5113,34168,
		51832,17158,65144,4000576,75720,50472,4000989,34742,11001394,23853,2162,4000438
	)
);

-- filtered orgs
-- locale: only include 'us','mx','ca','pr'
-- release type: exclude phase 2
-- doesn't exclude modi for now
-- TODO: decide on whether we want to exclude modi orgs, if so we should update this query every time there's an updated modi list
CREATE OR REPLACE TEMP VIEW filtered_orgs AS (
	SELECT o.id AS org_id, oc.cell_id, o.created_at, o.release_type_enum from clouddb.organizations o
	INNER JOIN clouddb.org_cells oc ON o.id = oc.org_id
	WHERE o.internal_type = 0 -- customer orgs
	AND o.locale in ('us','mx','ca','pr')
	AND o.release_type_enum <> 1 -- phase 2
	-- AND o.id NOT IN (modi list)
);

-- net new orgs
CREATE OR REPLACE TEMP VIEW net_new_orgs AS (
	SELECT org_id, created_at AS opt_in_date, 'net new' AS cohort from filtered_orgs
		WHERE created_at >= '2022-11-07'
);

-- 11/14
CREATE OR REPLACE TEMP VIEW ga_group_11_14_orgs AS (
	SELECT org_id, TO_DATE('11-14-2022','MM-dd-yyyy') AS opt_in_date, '11/14 group' AS cohort from filtered_orgs
	WHERE created_at < '2022-11-07'
	AND cell_id in ('us5', 'us4')
	AND org_id NOT IN (
		SELECT org_id from self_served_orgs
	)
	AND org_id NOT IN (
		SELECT org_id from closed_beta_orgs
	)
);

-- 11/14 EA
CREATE OR REPLACE TEMP VIEW ga_group_11_14_ea_orgs AS (
	SELECT org_id, TO_DATE('11-14-2022','MM-dd-yyyy') AS opt_in_date, '11/14 group' AS cohort from filtered_orgs
	WHERE created_at < '2022-11-07'
	AND release_type_enum = 2 -- Early Adopters
	AND org_id NOT IN (
		SELECT org_id from self_served_orgs
	)
	AND org_id NOT IN (
		SELECT org_id from closed_beta_orgs
	)
);

-- 12/5
CREATE OR REPLACE TEMP VIEW ga_group_12_5_orgs AS (
	SELECT org_id, TO_DATE('12-05-2022','MM-dd-yyyy') AS opt_in_date, '12/5 group' AS cohort from filtered_orgs
	WHERE created_at < '2022-11-07'
	AND cell_id in ('us3', 'us8', 'us9')
	AND org_id NOT IN (
		SELECT org_id from self_served_orgs
	)
	AND org_id NOT IN (
		SELECT org_id from closed_beta_orgs
	)
);

-- 12/12
CREATE OR REPLACE TEMP VIEW ga_group_12_12_orgs AS (
	SELECT org_id, TO_DATE('12-12-2022','MM-dd-yyyy') AS opt_in_date, '12/12 group' AS cohort from filtered_orgs
	WHERE created_at < '2022-11-07'
	AND cell_id in ('us6', 'us7', 'us10', 'us11')
	AND org_id NOT IN (
		SELECT org_id from self_served_orgs
	)
	AND org_id NOT IN (
		SELECT org_id from closed_beta_orgs
	)
);


-- UNION
CREATE OR REPLACE TEMP VIEW hev2_fully_opted_in_orgs AS (
	SELECT * FROM self_served_orgs
	UNION ALL
		SELECT * FROM closed_beta_orgs
	UNION ALL
		SELECT * FROM net_new_orgs
	UNION ALL
		SELECT * FROM ga_group_11_14_ea_orgs
	UNION ALL
		SELECT * FROM ga_group_11_14_orgs
	UNION ALL
		SELECT * FROM ga_group_12_5_orgs
	UNION ALL
		SELECT * FROM ga_group_12_12_orgs
);

-- COMMAND ----------

create table
if not exists dataprep_safety.hev2_fully_opted_in_orgs using delta
as
select * from hev2_fully_opted_in_orgs;

insert overwrite table dataprep_safety.hev2_fully_opted_in_orgs (
  select * from hev2_fully_opted_in_orgs
);
