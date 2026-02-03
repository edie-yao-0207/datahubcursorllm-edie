queries = {
    "Orgs with the XXX (replace with what you are looking for) feature flag enabled": """
     SELECT fpss.org_id
     FROM releasemanagementdb_shards.feature_package_self_serve fpss -- look for which orgs have it enabled
     INNER JOIN releasemanagementdb_shards.feature_packages fp -- look for unique feature flags
       ON fpss.feature_package_uuid = fp.uuid
     WHERE fp.ld_feature_key = 'XXX' -- insert feature flag name of interest
       AND enabled = 1 -- org is enabled
       AND fp.deleted = 0 -- feature is not deleted
       AND fpss.scope = 1 -- org-scoped
            """
}
