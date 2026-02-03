from datamodel.ops.datahub.highlighted_queries.queries.datamodel_core.lifetime_device_activity import (
    queries as lifetime_device_activity_queries,
)

queries = {}

for k, v in lifetime_device_activity_queries.items():
    k = k.replace("Active Devices", "Online Devices")
    queries[k] = v.replace(
        "datamodel_core.lifetime_device_activity",
        "datamodel_core.lifetime_device_online",
    )
