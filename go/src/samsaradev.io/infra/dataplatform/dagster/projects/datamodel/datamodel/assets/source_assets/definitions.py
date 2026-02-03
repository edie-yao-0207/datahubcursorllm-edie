from ...common.constants import ReplicationGroups
from ...common.utils import build_source_assets, get_all_regions

definitions_products = build_source_assets(
    name="products",
    database="definitions",
    regions=get_all_regions(),
    group_name=ReplicationGroups.DATAPIPELINES,
    description="Mapping from Product ID to human-readable names and Product Group ID.",
)


definitions_harsh_accel_type_enums = build_source_assets(
    name="harsh_accel_type_enums",
    database="definitions",
    regions=get_all_regions(),
    group_name=ReplicationGroups.DATAPIPELINES,
    description="Maps the enum HarshAccelTypeEnum from hubproto to the corresponding harsh event /n/n[HubProto Definitions](https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/hubproto/object_stat.pb.go#L71)",
)
