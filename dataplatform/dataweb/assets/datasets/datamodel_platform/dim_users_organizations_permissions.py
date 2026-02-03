from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    table
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "user_id", "tag_id", "role_id", "custom_role_uuid"]

dim_users_organizations_permissions_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "ID of Samsara Cloud Dashboard user"
        },
    },
    {
        "name": "tag_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "ID of tag associated with user role"
        },
    },
    {
        "name": "role_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Type of role (built_in or custom)"
        },
    },
    {
        "name": "role_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "ID of role (mainly for built-in roles)"
        },
    },
    {
        "name": "custom_role_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for custom roles"
        },
    },
    {
        "name": "role_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of role"
        },
    },
    {
        "name": "email",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Email of user"
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of user"
        },
    },
    {
        "name": "expire_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Time at which the user`s role assignment expires"
        },
    },
    {
        "name": "permission_sets",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "permission_set_id", "type": "string", "nullable": True},
                    {"name": "view_access", "type": "boolean", "nullable": True},
                    {"name": "edit_access", "type": "boolean", "nullable": True},
                    {"name": "create_access", "type": "boolean", "nullable": True},
                    {"name": "update_access", "type": "boolean", "nullable": True},
                    {"name": "delete_access", "type": "boolean", "nullable": True},
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of permission sets granted to the user with their access levels"
        },
    },
    {
        "name": "permissions",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {"name": "permission_id", "type": "string", "nullable": True},
                    {"name": "view_access", "type": "boolean", "nullable": True},
                    {"name": "edit_access", "type": "boolean", "nullable": True},
                    {"name": "create_access", "type": "boolean", "nullable": True},
                    {"name": "update_access", "type": "boolean", "nullable": True},
                    {"name": "delete_access", "type": "boolean", "nullable": True},
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of individual permissions granted to the user with their access levels"
        },
    },
    {
        "name": "actions",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of actions the user has access to based on their permissions"
        },
    },
]


dim_users_organizations_permissions_query = """
WITH permission_mapping AS (
    SELECT DISTINCT
        permission_set_id,
        permission_id
    FROM definitions.permission_actions
),
user_role_assignments AS (
    SELECT
        rcuo.user_id,
        rcuo.organization_id,
        rcuo.role_id,
        rcuo.custom_role_uuid,
        rcuo.tag_id,
        rcuo.expire_at,
        rcu.email,
        rcu.name
    FROM datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
    JOIN datamodel_platform_bronze.raw_clouddb_users rcu
        ON rcuo.user_id = rcu.id
        AND rcuo.date = rcu.date
    JOIN clouddb.tags t -- filter to only active tags
        ON rcuo.organization_id = t.org_id
        AND rcuo.tag_id = t.id
    WHERE
        rcuo.date = '{PARTITION_START}'
        AND rcuo.tag_id IS NOT NULL

    UNION ALL

    SELECT
        rcuo.user_id,
        rcuo.organization_id,
        rcuo.role_id,
        rcuo.custom_role_uuid,
        rcuo.tag_id,
        rcuo.expire_at,
        rcu.email,
        rcu.name
    FROM datamodel_platform_bronze.raw_clouddb_users_organizations rcuo
    JOIN datamodel_platform_bronze.raw_clouddb_users rcu
        ON rcuo.user_id = rcu.id
        AND rcuo.date = rcu.date
    WHERE
        rcuo.date = '{PARTITION_START}'
        AND rcuo.tag_id IS NULL
),
built_in_role_permission_set_grants AS (
    -- Permission set grants: permission is derived (for action calculation only)
    SELECT
        ura.user_id,
        ura.organization_id,
        ura.tag_id,
        'built_in' AS role_type,
        ura.role_id,
        ura.custom_role_uuid,
        ura.email,
        ura.name,
        ura.expire_at,
        birp.role_name,
        birp.grant_id AS permission_set_id,
        pm.permission_id,
        FALSE AS is_direct_permission,  -- permission is derived from permission set
        birp.view_access,
        birp.edit_access,
        birp.create_access,
        birp.update_access,
        birp.delete_access
    FROM user_role_assignments ura
    JOIN definitions.built_in_roles_permissions birp
        ON ura.role_id = birp.role_id
    JOIN permission_mapping pm
        ON birp.grant_id = pm.permission_set_id
    WHERE
        ura.role_id IS NOT NULL
        AND ura.custom_role_uuid IS NULL
        AND birp.grant_type = 'permission_set'
),
built_in_role_permission_grants AS (
    -- Individual permission grants: permission_set_id is NULL since no set was granted
    SELECT
        ura.user_id,
        ura.organization_id,
        ura.tag_id,
        'built_in' AS role_type,
        ura.role_id,
        ura.custom_role_uuid,
        ura.email,
        ura.name,
        ura.expire_at,
        birp.role_name,
        CAST(NULL AS STRING) AS permission_set_id,
        birp.grant_id AS permission_id,
        TRUE AS is_direct_permission,  -- permission is directly granted
        birp.view_access,
        birp.edit_access,
        birp.create_access,
        birp.update_access,
        birp.delete_access
    FROM user_role_assignments ura
    JOIN definitions.built_in_roles_permissions birp
        ON ura.role_id = birp.role_id
    WHERE
        ura.role_id IS NOT NULL
        AND ura.custom_role_uuid IS NULL
        AND birp.grant_type = 'permission'
),
custom_role_permission_set_grants AS (
    -- Permission set grants: permission is derived (for action calculation only)
    SELECT
        exploded.user_id,
        exploded.organization_id,
        exploded.tag_id,
        exploded.role_type,
        exploded.role_id,
        exploded.custom_role_uuid,
        exploded.email,
        exploded.name,
        exploded.expire_at,
        exploded.role_name,
        exploded.permission_set_id,
        pm.permission_id,
        FALSE AS is_direct_permission,  -- permission is derived from permission set
        exploded.view_access,
        exploded.edit_access,
        exploded.create_access,
        exploded.update_access,
        exploded.delete_access
    FROM (
        SELECT
            ura.user_id,
            ura.organization_id,
            ura.tag_id,
            'custom' AS role_type,
            ura.role_id,
            ura.custom_role_uuid,
            ura.email,
            ura.name,
            ura.expire_at,
            cr.name AS role_name,
            pset.id AS permission_set_id,
            pset.view AS view_access,
            pset.edit AS edit_access,
            pset.create AS create_access,
            pset.update AS update_access,
            pset.delete AS delete_access
        FROM user_role_assignments ura
        JOIN datamodel_platform_bronze.raw_clouddb_custom_roles cr
            ON ura.custom_role_uuid = cr.uuid
            AND ura.organization_id = cr.org_id
        LATERAL VIEW EXPLODE(cr.permissions.permission_sets) AS pset
        WHERE
            ura.custom_role_uuid IS NOT NULL
            AND cr.date = '{PARTITION_START}'
        ) exploded
    JOIN permission_mapping pm
        ON exploded.permission_set_id = pm.permission_set_id
),
custom_role_permission_grants AS (
    -- Individual permission grants: permission_set_id is NULL since no set was granted
    SELECT
        ura.user_id,
        ura.organization_id,
        ura.tag_id,
        'custom' AS role_type,
        ura.role_id,
        ura.custom_role_uuid,
        ura.email,
        ura.name,
        ura.expire_at,
        cr.name AS role_name,
        CAST(NULL AS STRING) AS permission_set_id,
        perm.id AS permission_id,
        TRUE AS is_direct_permission,  -- permission is directly granted
        perm.view AS view_access,
        perm.edit AS edit_access,
        perm.create AS create_access,
        perm.update AS update_access,
        perm.delete AS delete_access
    FROM user_role_assignments ura
    JOIN datamodel_platform_bronze.raw_clouddb_custom_roles cr
        ON ura.custom_role_uuid = cr.uuid
        AND ura.organization_id = cr.org_id
    LATERAL VIEW EXPLODE(cr.permissions.permissions) AS perm
    WHERE ura.custom_role_uuid IS NOT NULL
        AND cr.date = '{PARTITION_START}'
),
all_user_permissions AS (
    SELECT * FROM built_in_role_permission_set_grants

    UNION ALL

    SELECT * FROM built_in_role_permission_grants

    UNION ALL

    SELECT * FROM custom_role_permission_set_grants

    UNION ALL

    SELECT * FROM custom_role_permission_grants
),
-- Aggregate permission_sets and permissions
-- permission_sets: Only directly granted permission sets (filter out NULLs from individual permission grants)
-- permissions: Only directly granted permissions (filter out derived permissions from permission sets)
user_permissions_agg AS (
    SELECT
        user_id,
        organization_id,
        tag_id,
        role_type,
        role_id,
        custom_role_uuid,
        email,
        name,
        MAX(expire_at) AS expire_at,
        role_name,
        FILTER(
            COLLECT_SET(
                STRUCT(
                    permission_set_id,
                    view_access,
                    edit_access,
                    create_access,
                    update_access,
                    delete_access
                )
            ),
            x -> x.permission_set_id IS NOT NULL
        ) AS permission_sets,
        FILTER(
            COLLECT_SET(
                CASE WHEN is_direct_permission THEN
                    STRUCT(
                        permission_id,
                        view_access,
                        edit_access,
                        create_access,
                        update_access,
                        delete_access
                    )
                END
            ),
            x -> x IS NOT NULL AND x.permission_id IS NOT NULL
        ) AS permissions
    FROM all_user_permissions
    GROUP BY
        user_id,
        organization_id,
        tag_id,
        role_type,
        role_id,
        custom_role_uuid,
        email,
        name,
        role_name
),
-- Aggregate actions separately
-- Include ALL actions (from both direct permissions and permissions derived from permission sets)
-- This ensures we capture all actions a user can perform, regardless of how they got access
user_actions_agg AS (
    SELECT
        aup.user_id,
        aup.organization_id,
        aup.tag_id,
        aup.role_type,
        aup.role_id,
        aup.custom_role_uuid,
        aup.role_name,
        aup.email,
        aup.name,
        MAX(aup.expire_at) AS expire_at,
        COLLECT_SET(pa.action) AS actions
    FROM all_user_permissions aup
    JOIN definitions.permission_actions pa
        ON aup.permission_id = pa.permission_id
    WHERE
        (aup.view_access = TRUE AND pa.access_level = 'read')
        OR (aup.edit_access = TRUE AND pa.access_level = 'write')
        OR (aup.create_access = TRUE AND pa.access_level = 'create')
        OR (aup.update_access = TRUE AND pa.access_level = 'update')
        OR (aup.delete_access = TRUE AND pa.access_level = 'delete')
    GROUP BY
        aup.user_id,
        aup.organization_id,
        aup.tag_id,
        aup.role_type,
        aup.role_id,
        aup.custom_role_uuid,
        aup.role_name,
        aup.email,
        aup.name
)
-- Join the two aggregations
SELECT
    '{PARTITION_START}' AS date,
    p.organization_id AS org_id,
    p.user_id,
    p.tag_id,
    p.role_type,
    p.role_id,
    p.custom_role_uuid,
    p.role_name,
    p.email,
    p.name,
    p.expire_at,
    p.permission_sets,
    p.permissions,
    a.actions
FROM user_permissions_agg p
LEFT JOIN user_actions_agg a
    ON p.user_id = a.user_id
    AND p.organization_id = a.organization_id
    AND COALESCE(p.tag_id, -1) = COALESCE(a.tag_id, -1)
    AND p.role_type = a.role_type
    AND COALESCE(p.role_id, -1) = COALESCE(a.role_id, -1)
    AND COALESCE(p.custom_role_uuid, '') = COALESCE(a.custom_role_uuid, '')
    AND COALESCE(p.role_name, '') = COALESCE(a.role_name, '')
    AND COALESCE(p.email, '') = COALESCE(a.email, '')
    AND COALESCE(p.name, '') = COALESCE(a.name, '')
    AND COALESCE(CAST(p.expire_at AS STRING), '') = COALESCE(CAST(a.expire_at AS STRING), '')
"""


@table(
    database=Database.DATAMODEL_PLATFORM,
    description=build_table_description(
        table_desc="""Dimension table mapping users to their permissions, permission sets, and actions based on their role assignments (built-in or custom roles).""",
        row_meaning="""Each row represents a user`s role assignment within an organization, including their granted permission sets, individual permissions, and derived actions.""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=dim_users_organizations_permissions_schema,
    upstreams=[
        "definitions.permission_actions",
        "datamodel_platform_bronze.raw_clouddb_users_organizations",
        "definitions.built_in_roles_permissions",
        "datamodel_platform_bronze.raw_clouddb_custom_roles",
        "datamodel_platform_bronze.raw_clouddb_users",
        "clouddb.tags"
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    backfill_start_date="2026-01-01",
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_users_organizations_permissions"),
        PrimaryKeyDQCheck(name="dq_pk_dim_users_organizations_permissions", primary_keys=PRIMARY_KEYS, block_before_write=True),
        NonNullDQCheck(name="dq_non_null_dim_users_organizations_permissions", non_null_columns=["date", "org_id", "user_id", "role_type"], block_before_write=True)
    ],
)
def dim_users_organizations_permissions(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = dim_users_organizations_permissions_query.format(
        PARTITION_START=PARTITION_START,
    )
    context.log.info(f"{query}")

    return query
