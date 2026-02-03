queries = {
    "Organizations that changed their camera settings in the last month": """
            WITH camera_settings_changes AS (
            SELECT
                `date`,
                org_id,
                settings_hash_id,
                LAG(settings_hash_id) OVER (PARTITION BY org_id ORDER BY `date` ASC) AS previous_settings_hash_id
            FROM datamodel_core_silver.stg_organizations_camera_settings
            WHERE `date` >= DATE_SUB(CURRENT_DATE(), 28)
                AND `date` <= CURRENT_DATE()
            )
            SELECT *
            FROM camera_settings_changes
            WHERE settings_hash_id != previous_settings_hash_id
        """
}
