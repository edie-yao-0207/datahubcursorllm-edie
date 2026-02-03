/* This query is used to populate the org_metadata table in the perf_infra dataset, used by other queries, with the following columns:

- org_id
- name
- active_vg_count
- active_cm_count
- active_ag_count
- active_ig_count
- num_dashboard_users
- driver_count
- tag_count
- soft_trailer_count

We have soft_trailer_count (product_id = 63), as per https://samsaradev.atlassian.net/browse/PI-194
In the Route Overview performance dashboard, we show metadata about the fastest and slowest orgs for that route.
We calculate how many soft trailersthe org has, because this can impact performance on certain pages, and be a key indicator.
Customers using soft trailers create a lot of them (soft trailers don’t need to have a gateway attached to be created in our system),
they might often be the cause of performance issues.
Typically if you see degraded performance for a service querying the customers’ assets (including soft trailers) and that customer has 500K soft trailers, then it is very likely that the performance issue comes from the number of soft trailers.
soft_trailer_count is the number of soft trailers that the org has, used in `route_overview/perf_infra_route_overview_relative_orgs.sql`
The soft trailers check could be reviewd and removed in the future.
 */
WITH
  org_shape AS (
    SELECT
      cs.org_id,
      cdbo.name,
      cs.num_vg_active AS active_vg_count,
      cs.num_cm_active AS active_cm_count,
      cs.num_ag_active AS active_ag_count,
      cs.num_ig_active AS active_ig_count,
      COALESCE(cs.total_dashboard_users, 0) AS num_dashboard_users
    FROM
      customer360.customer_360_snapshot AS cs
      INNER JOIN clouddb.organizations AS cdbo ON cdbo.id = cs.org_id
  ),
  org_drivers AS (
    SELECT
      org_id,
      COUNT(*) AS count
    FROM
      clouddb.drivers
    GROUP BY
      org_id
  ),
  org_tags AS (
    SELECT
      org_id,
      COUNT(*) AS count
    FROM
      clouddb.tags
    GROUP BY
      org_id
  ),
  org_soft_trailers AS (
    SELECT
      org_id,
      COUNT(*) AS count
    FROM
      productsdb.devices
    WHERE
      product_id = 63 -- soft trailers
    GROUP BY
      org_id
  )
SELECT
  os.*,
  COALESCE(od.count, 0) AS driver_count,
  COALESCE(ot.count, 0) AS tag_count,
  COALESCE(ost.count, 0) AS soft_trailer_count
FROM
  org_shape os
  INNER JOIN org_drivers od USING (org_id)
  INNER JOIN org_tags ot USING (org_id)
  LEFT JOIN org_soft_trailers ost USING (org_id) -- if there are no soft trailers, we still want to include the org, soft_trailer_count will be 0