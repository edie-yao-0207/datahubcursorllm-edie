from datetime import datetime

from ...common.constants import ReplicationGroups
from ...common.utils import build_source_assets, get_all_regions

productsdb_devices = build_source_assets(
    name="devices",
    database="productsdb",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    description="This table contains information about devices, which represents a customer's device (e.g a vehicle, or a trailer). The [productsdb.gateways](https://amundsen.internal.samsara.com/table_detail/cluster/delta/productsdb/gateways) table represents the actual Samsara hardware device",
)
