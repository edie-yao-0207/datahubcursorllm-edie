from datamodel.ops.datahub.highlighted_queries.queries.datamodel_core.dim_devices import (
    queries as dim_devices_queries,
)

queries = {}

for k, v in dim_devices_queries.items():
    queries[k] = v.replace(
        "datamodel_core.dim_devices", "datamodel_core.dim_devices_sensitive"
    )
