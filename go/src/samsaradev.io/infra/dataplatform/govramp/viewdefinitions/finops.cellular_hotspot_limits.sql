select
  id as org_id,
  name as org_name,
  created_at,
  updated_at,
  wifi_hotspot_data_cap_bytes_per_gateway,
  feature_id as hotspot_enforce_feature_id,
  activated_at_ms,
  expire_at_ms
from
  clouddb.organizations
    LEFT JOIN (
      SELECT
        feature_id,
        organization_id,
        activated_at_ms,
        expire_at_ms
      FROM
        clouddb.features_orgs
      where
        feature_id = 228 --https://github.com/samsara-dev/backend/blob/3fc42c470d5310066058760d8658ca40b8b3952b/go/src/samsaradev.io/cloudfeatures/legacyfeatures.go#L145
    ) hs_ff
      ON organizations.id = hs_ff.organization_id