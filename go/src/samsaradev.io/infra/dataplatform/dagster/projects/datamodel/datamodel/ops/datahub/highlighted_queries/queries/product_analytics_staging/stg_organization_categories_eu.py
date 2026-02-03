from datamodel.ops.datahub.highlighted_queries.queries.product_analytics_staging.stg_organization_categories_global import (
    queries as stg_organization_categories_global_queries,
)

queries = {}

for k, v in stg_organization_categories_global_queries.items():
    queries[k] = v.replace(
        "product_analytics_staging.stg_organization_categories_global",
        "product_analytics_staging.stg_organization_categories_eu",
    )
