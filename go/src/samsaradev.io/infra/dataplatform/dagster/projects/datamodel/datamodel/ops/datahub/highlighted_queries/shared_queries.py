from dataclasses import dataclass


@dataclass
class SharedHighlightedQuery:
    """A highlighted query that we want to share across multiple tables"""

    name: str
    sql: str


users_and_their_roles = SharedHighlightedQuery(
    name="See all users and their roles (system roles or custom roles)",
    sql="""
WITH dim_users AS (
    SELECT *
    FROM datamodel_platform.dim_users
    WHERE date = (SELECT max(date) FROM datamodel_platform.dim_users)
    ),

    dim_users_organizations AS (
    SELECT *
    FROM datamodel_platform.dim_users_organizations
    -- Get the date that we're using from the users table, so that we match the users table
    WHERE date = (SELECT max(date) FROM dim_users)
),

-- List all roles a user has, whether they be "system" (i.e samsara built in) roles, or custom roles
user_roles AS (
    SELECT
        uo.user_id,
        uo.org_id,
        uo.org_name,
        uo.date,
        role_id AS role_id_or_uuid,
        'system_role' AS role_type
    FROM
        dim_users_organizations uo
        LATERAL VIEW explode(role_ids) AS role_id

    UNION ALL

    SELECT
        uo.user_id,
        uo.org_id,
        uo.org_name,
        uo.date,
        custom_role_uuid AS role_id_or_uuid,
        'custom_role' AS role_type
    FROM
        dim_users_organizations uo
        LATERAL VIEW explode(custom_role_uuids) AS custom_role_uuid
)

SELECT
    user_roles.user_id,
    user_roles.org_id,
    user_roles.org_name,
    user_roles.role_id_or_uuid,
    user_roles.role_type,
    custom_roles.name as custom_role_name
FROM
    user_roles
LEFT OUTER JOIN clouddb.custom_roles
    ON user_roles.role_id_or_uuid = custom_roles.uuid AND user_roles.role_type = 'custom_role'
ORDER BY
    user_roles.user_id,
    user_roles.org_id,
    user_roles.role_id_or_uuid;
""",
)
