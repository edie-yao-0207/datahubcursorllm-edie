from datetime import datetime

from ...common.constants import ReplicationGroups
from ...common.utils import AWSRegions, build_source_assets, get_all_regions

clouddb_organizations = build_source_assets(
    name="organizations",
    database="clouddb",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    description="This table contains configuration data and metadata associated with a Samsara customer.",
)
