-- Return the list of orgs allocated for the Big Orgs testing initiative.
-- Currently this is hard-coded in various notebooks etc. - this view
-- is intended to provide a centralised list instead.
SELECT
  id AS org_id,
  name
FROM
  clouddb.organizations
WHERE id IN (
   45402, -- Sysco Corporation
   31577, -- Clean Harbors
   74597, -- XPO LTL
   7004969, -- USIC
   20413, -- Estes Express Lines
   2000012, -- big-vehicle-test-org
   2000013, -- Replicated Org
   2000016, -- big-vehicle-test-org-v2
   2000099 -- STCE Replicated Org
)
