queries = {
    "See all permissions and actions for a user`s role": """
    SELECT
        org_id,
        user_id,
        tag_id,
        role_type,
        role_id,
        custom_role_uuid,
        email,
        name,
        permission_sets,
        permissions,
        actions
    FROM datamodel_platform.dim_users_organizations_permissions
    WHERE date = (SELECT MAX(date) FROM datamodel_platform.dim_users_organizations_permissions)
    """
}
