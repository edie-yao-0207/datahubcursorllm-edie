from ...common.constants import ReplicationGroups
from ...common.utils import Database, build_source_assets, get_all_regions

datamodel_core_silver_stg_product_variants = build_source_assets(
    name="stg_product_variants",
    database=Database.DATAMODEL_CORE_SILVER,
    regions=get_all_regions(),
    group_name=ReplicationGroups.DATAPIPELINES,
    partition_keys=[],
    description="Mapping from Variant ID and corresponding Product ID to human-readable names",
)
