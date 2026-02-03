from datamodel.ops.datahub.highlighted_queries.shared_queries import (
    users_and_their_roles,
)

queries = {}

queries[users_and_their_roles.name] = users_and_their_roles.sql
