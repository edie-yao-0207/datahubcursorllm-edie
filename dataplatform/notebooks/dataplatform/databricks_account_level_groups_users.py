# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook Setup
# COMMAND ----------

# Schema selection widget - allows switching between dataplatform and dataplatform_dev
dbutils.widgets.dropdown(
    name="schema_name",
    defaultValue="dataplatform",
    choices=["dataplatform", "dataplatform_dev"],
    label="Schema Name",
)
schema_name = dbutils.widgets.get("schema_name")
print(f"Writing data to: {schema_name}.*")

# COMMAND ----------

# Imports
from databricks.sdk import AccountClient
from pyspark.sql import Row, DataFrame
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Set up Databricks AccountClient

# Note since this is an account-level notebook, we need to set up auth
# despite running in a notebook on Databricks. And account-level APIs cannot use PATs,
# so we use OAuth M2M authentication with the oauth info from a service principal: dataplatform-notebooks-account-level
scope = "dataplatform-notebooks-account-level-sp-secrets"
client_id = dbutils.secrets.get(scope=scope, key="oauth_client_id")
client_secret = dbutils.secrets.get(scope=scope, key="oauth_client_secret")

ACCOUNT_ID = "f8e9c6b3-6083-4e24-bf92-19bbd19235e3"

# Initialize AccountClient for account-level access with OAuth M2M authentication
account_client = AccountClient(
    host="https://accounts.cloud.databricks.com/",
    account_id=ACCOUNT_ID,
    client_id=client_id,
    client_secret=client_secret,
    auth_type="oauth-m2m",
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Databricks Groups and Users related tables
# COMMAND ----------

# Get all groups at account level
print("Fetching account-level groups...")
groups = list(account_client.groups.list())
print(f"Found {len(groups)} groups")

# COMMAND ----------

# Get all users at account level
print("Fetching account-level users...")
users = list(account_client.users.list())
print(f"Found {len(users)} users")


# COMMAND ----------


def summarize_dataframe(df, dataframe_name):
    print(f"Created {dataframe_name} DataFrame with {df.count()} rows")
    df.show(5, truncate=False)


# COMMAND ----------

# Create groups DataFrame
groups_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("groupName", StringType(), True),
    ]
)

# Create groups DataFrame with only required fields
groups_data = []
for group in groups:
    group_dict = group.as_dict() if hasattr(group, "as_dict") else group.__dict__
    groups_data.append(
        {
            "id": group_dict.get("id"),
            "groupName": group_dict.get("displayName")
            or group_dict.get("display_name"),
        }
    )

groups_df = spark.createDataFrame(groups_data, schema=groups_schema)
groups_dataframe_name = "Groups dataframe"
summarize_dataframe(groups_df, groups_dataframe_name)

# COMMAND ----------

# Create users DataFrame
users_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("email", StringType(), True),
        StructField("active", StringType(), True),
    ]
)

# Create users DataFrame with only required fields
users_data = []
for user in users:
    user_dict = user.as_dict() if hasattr(user, "as_dict") else user.__dict__
    users_data.append(
        {
            "id": user_dict.get("id"),
            "displayName": user_dict.get("displayName")
            or user_dict.get("display_name"),
            "email": user_dict.get("userName"),
            "active": user_dict.get("active"),
        }
    )

users_df = spark.createDataFrame(users_data, schema=users_schema)
users_dataframe_name = "Users dataframe"
summarize_dataframe(users_df, users_dataframe_name)

# COMMAND ----------

# Create a mapping of user_id to user_email for efficient lookup
user_id_to_email = {}
for user in users:
    user_dict = user.as_dict() if hasattr(user, "as_dict") else user.__dict__
    user_id = user_dict.get("id")
    user_email = user_dict.get("userName")  # userName field contains the email
    if user_id and user_email:
        user_id_to_email[user_id] = user_email

print(f"Created user ID to email mapping for {len(user_id_to_email)} users")


# COMMAND ----------

# Create Group to Members List

# Create simple group memberships table (no recursion - just direct members)
print("Creating group to members dataframe...")

group_memberships = []
for group in groups:
    group_dict = group.as_dict() if hasattr(group, "as_dict") else group.__dict__
    group_id = group_dict.get("id")
    group_name = group_dict.get("displayName", group_dict.get("display_name"))

    try:
        group_details = account_client.groups.get(group_id)
        members = group_details.members if hasattr(group_details, "members") else []

        for member in members:
            member_dict = (
                member.as_dict() if hasattr(member, "as_dict") else member.__dict__
            )
            member_value = member_dict.get("value")
            member_ref = member_dict.get("$ref", "")

            # Determine member type from the ref field since type is None
            if member_ref.startswith("Users/"):
                member_type = "User"
            elif member_ref.startswith("Groups/"):
                member_type = "Group"
            elif member_ref.startswith("ServicePrincipals/"):
                member_type = "ServicePrincipal"
            else:
                member_type = "Unknown"

            group_memberships.append(
                {
                    "group_id": group_id,
                    "group_name": group_name,
                    "member_id": member_value,
                    "member_display": member_dict.get("display"),
                    "member_type": member_type,
                    "member_ref": member_ref,
                }
            )
    except Exception as e:
        print(f"Error getting members for group {group_name} ({group_id}): {e}")

groups_to_members_df = spark.createDataFrame(group_memberships)
groups_to_members_dataframe_name = "Groups to members dataframe"
summarize_dataframe(groups_to_members_df, groups_to_members_dataframe_name)


# COMMAND ----------


def write_df_to_delta(df, table_name, dataframe_name):
    print(f"Writing {dataframe_name} to {schema_name}.{table_name}")

    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{schema_name}.{table_name}"
    )
    print(f"{dataframe_name} written successfully to {schema_name}.{table_name}")


# COMMAND ----------

# Write group to members dataframe to Delta table
write_df_to_delta(
    groups_to_members_df,
    "databricks_groups_to_members",
    groups_to_members_dataframe_name,
)


# COMMAND ----------


# Helper function: Genereate groups_to_members_flattened DataFrame
def generate_groups_to_members_flattened_df(groups_to_members_df) -> DataFrame:
    """
    Recursive approach to generate groups_to_members_flattened DataFrame.
    This avoids Spark SQL operations and uses Python recursion instead.

    Args:
        groups_to_members_df: DataFrame with group-to-member relationships

    Returns:
        DataFrame with columns: group_id, group_name, member_id, member_display, member_email, member_type
    """
    print(
        "Starting recursive approach to generate groups_to_members_flattened DataFrame..."
    )

    # Convert DataFrame to Python dictionaries for easier manipulation
    groups_to_members_data = groups_to_members_df.collect()

    # Create lookup dictionaries for efficiency
    group_id_to_name = {}
    member_id_to_email = {}

    # Build group ID to name mapping
    for row in groups_to_members_data:
        group_id_to_name[row.group_id] = row.group_name

    # Build member ID to email mapping from users DataFrame
    users_data = users_df.collect()
    for row in users_data:
        member_id_to_email[row.id] = row.email

    # Create group-to-members mapping
    group_members = {}
    for row in groups_to_members_data:
        group_id = row.group_id
        if group_id not in group_members:
            group_members[group_id] = []
        group_members[group_id].append(
            {
                "member_id": row.member_id,
                "member_display": row.member_display,
                "member_type": row.member_type,
            }
        )

    def get_all_members_in_group(
        group_id, visited_groups=None, seen_member_group_pairs=None
    ):
        """
        Recursively get all members (users and service principals) in a group, including members in sub-groups.
        Prevents duplicates by tracking seen member-group pairs.

        Args:
            group_id: The group ID to expand
            visited_groups: Set of group IDs already visited (for cycle detection)
            seen_member_group_pairs: Set of (member_id, group_id) pairs already seen

        Returns:
            List of member dictionaries
        """
        if visited_groups is None:
            visited_groups = set()
        if seen_member_group_pairs is None:
            seen_member_group_pairs = set()

        # Cycle detection
        if group_id in visited_groups:
            print(f"  Cycle detected for group {group_id}, skipping...")
            return []

        visited_groups.add(group_id)
        all_members = []

        if group_id not in group_members:
            return all_members

        # First, add all direct members (users and service principals) of this group
        for member in group_members[group_id]:
            if member["member_type"] in ["User", "ServicePrincipal"]:
                # Direct member - check for duplicates
                member_id = member["member_id"]
                member_group_pair = (member_id, group_id)

                if member_group_pair not in seen_member_group_pairs:
                    seen_member_group_pairs.add(member_group_pair)

                    # Get email for users, use display name for service principals
                    if member["member_type"] == "User":
                        member_email = member_id_to_email.get(member_id, "Unknown")
                    else:  # ServicePrincipal
                        member_email = member[
                            "member_display"
                        ]  # Service principals don't have emails

                    all_members.append(
                        {
                            "group_id": group_id,
                            "group_name": group_id_to_name.get(group_id, "Unknown"),
                            "member_id": member_id,
                            "member_display": member["member_display"],
                            "member_email": member_email,
                            "member_type": member["member_type"],
                        }
                    )

        # Then, recursively get members from sub-groups and add them to THIS group too
        for member in group_members[group_id]:
            if member["member_type"] == "Group":
                # Sub-group member - recurse to get all members in the sub-group
                sub_group_id = member["member_id"]
                sub_group_members = get_all_members_in_group(
                    sub_group_id, visited_groups.copy(), seen_member_group_pairs
                )

                # Add each member from the sub-group to THIS group as well
                for sub_member in sub_group_members:
                    member_id = sub_member["member_id"]
                    member_group_pair = (member_id, group_id)

                    if member_group_pair not in seen_member_group_pairs:
                        seen_member_group_pairs.add(member_group_pair)
                        all_members.append(
                            {
                                "group_id": group_id,
                                "group_name": group_id_to_name.get(group_id, "Unknown"),
                                "member_id": member_id,
                                "member_display": sub_member["member_display"],
                                "member_email": sub_member["member_email"],
                                "member_type": sub_member["member_type"],
                            }
                        )

        return all_members

    # Process all groups with per-group deduplication
    all_groups_members = []
    total_groups = len(group_members)

    for i, group_id in enumerate(group_members.keys()):
        if i % 50 == 0:  # Progress indicator
            print(f"Processing group {i+1}/{total_groups}...")

        # Each group gets its own deduplication set
        group_seen_pairs = set()
        group_members_list = get_all_members_in_group(
            group_id, seen_member_group_pairs=group_seen_pairs
        )
        all_groups_members.extend(group_members_list)

    print(f"Found {len(all_groups_members)} total group-member relationships")

    # Convert back to DataFrame and deduplicate at the end
    groups_to_members_flattened_df = spark.createDataFrame(all_groups_members)

    # Final deduplication to remove any remaining duplicates
    groups_to_members_flattened_df = groups_to_members_flattened_df.dropDuplicates(
        ["group_id", "member_id"]
    )

    return groups_to_members_flattened_df


# COMMAND ----------

# Generate groups_members DataFrame
groups_to_members_flattened_df = generate_groups_to_members_flattened_df(
    groups_to_members_df
)
groups_to_members_flattened_dataframe_name = "Groups to members flattened dataframe"
summarize_dataframe(
    groups_to_members_flattened_df, groups_to_members_flattened_dataframe_name
)

# COMMAND ----------

# Write groups to Delta table
write_df_to_delta(groups_df, "databricks_groups", groups_dataframe_name)

# COMMAND ----------

# Write users to Delta table
write_df_to_delta(users_df, "databricks_users", users_dataframe_name)

# COMMAND ----------

# Write groups_to_members_flattened many-to-many association table to Delta table
write_df_to_delta(
    groups_to_members_flattened_df,
    "databricks_groups_to_members_flattened",
    groups_to_members_flattened_dataframe_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # TODO: Move rest of notebook to Dagster
# MAGIC The rest of this notebook can be moved to Dagster, which could be cleaner/easier to add table descriptions
# MAGIC Leaving this here for now for quicker debugging/iteration, then will move to Dagster when done.

# COMMAND ----------

# MAGIC %md
# MAGIC # Create table of users with access to specific tables in "default" catalog
# MAGIC
# MAGIC This creates a table that shows which specific tables each user can query in the default catalog.
# MAGIC This is useful for dashboards where you want to show a user which tables they have access to.
# MAGIC
# MAGIC **Sample Dashboard Queries:**
# MAGIC
# MAGIC **1. User Summary (aggregated data):**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   user_email,
# MAGIC   user_display,
# MAGIC   department,
# MAGIC   job_family_group,
# MAGIC   table_count,
# MAGIC   schema_count
# MAGIC FROM dataplatform.databricks_users_with_access_to_default_tables
# MAGIC WHERE user_email = '{{user_email_parameter}}'
# MAGIC ```
# MAGIC
# MAGIC **2. Detailed Table Access (one row per table):**
# MAGIC ```sql
# MAGIC SELECT
# MAGIC   user_email,
# MAGIC   table_catalog,
# MAGIC   table_schema,
# MAGIC   table_name,
# MAGIC   full_table_name
# MAGIC FROM dataplatform.databricks_users_default_tables_access
# MAGIC WHERE user_email = '{{user_email_parameter}}'
# MAGIC ORDER BY table_schema, table_name
# MAGIC ```

# COMMAND ----------

# Create table showing which specific tables each user can query
print("Creating users with access to default tables dataframe")

# This creates two tables in sequence:
# 1. databricks_users_default_tables_access: One row per user-table combination (detailed view)
# 2. databricks_users_with_access_to_default_tables: Aggregated by user with counts (reads from #1)
# Key benefits:
# 1. Pre-computed and persisted for fast dashboard queries
# 2. Includes both direct user permissions and group-based permissions
# 3. Enforces hierarchical permissions (USE_CATALOG + USE_SCHEMA + SELECT)
# 4. Includes employee hierarchy data (department, job_family_group)
# 5. Two levels of detail: detailed table access and aggregated summaries

qualified_table_access_query = f"""
WITH catalog_use_grantees AS (
    -- Users/groups with USE_CATALOG on default catalog
    SELECT DISTINCT grantee
    FROM system.information_schema.catalog_privileges
    WHERE catalog_name = 'default'
    AND privilege_type IN ('USE_CATALOG', 'ALL_PRIVILEGES')
),
schema_use_grantees AS (
    -- Users/groups with USE_SCHEMA on schemas in default catalog
    -- Note that we don't need to check for USE_SCHEMA on the default catalog directly since
    -- privileges are inherited from the catalog and when they are, a row shows up
    -- in the schema_privileges table for each schema in the catalog.
    SELECT DISTINCT grantee
    FROM system.information_schema.schema_privileges
    WHERE catalog_name = 'default'
    AND privilege_type IN ('USE_SCHEMA', 'ALL_PRIVILEGES')
),
table_select_grantees AS (
    -- Users/groups with SELECT on tables in default catalog
    -- Note that we don't need to check for SELECT on the catalog or schema directly since
    -- privileges are inherited from the catalog and schema and when they are, a row shows up
    -- in the table_privileges table for each table in the schema or catalog.
    SELECT DISTINCT
        grantee,
        table_catalog,
        table_schema,
        table_name,
        CONCAT(table_catalog, '.', table_schema, '.', table_name) as full_table_name
    FROM system.information_schema.table_privileges
    WHERE table_catalog = 'default'
    AND privilege_type IN ('SELECT', 'ALL_PRIVILEGES')
),
-- Expand groups to individual users for each permission type
catalog_use_users AS (
    -- Direct users with USE_CATALOG
    SELECT DISTINCT u.id as user_id, u.email as user_email
    FROM {schema_name}.databricks_users u
    INNER JOIN catalog_use_grantees cug ON u.id = cug.grantee

    UNION

    -- Users in groups with USE_CATALOG
    SELECT DISTINCT
        gtmf.member_id as user_id,
        gtmf.member_email as user_email
    FROM {schema_name}.databricks_groups_to_members_flattened gtmf
    INNER JOIN catalog_use_grantees cug ON gtmf.group_name = cug.grantee
    WHERE gtmf.member_type = 'User'
),
schema_use_users AS (
    -- Direct users with USE_SCHEMA
    SELECT DISTINCT u.id as user_id, u.email as user_email
    FROM {schema_name}.databricks_users u
    INNER JOIN schema_use_grantees sug ON u.id = sug.grantee

    UNION

    -- Users in groups with USE_SCHEMA
    SELECT DISTINCT
        gtmf.member_id as user_id,
        gtmf.member_email as user_email
    FROM {schema_name}.databricks_groups_to_members_flattened gtmf
    INNER JOIN schema_use_grantees sug ON gtmf.group_name = sug.grantee
    WHERE gtmf.member_type = 'User'
),
table_select_users AS (
    -- Direct users with SELECT on specific tables
    SELECT DISTINCT
        u.id as user_id,
        u.email as user_email,
        tsg.table_catalog,
        tsg.table_schema,
        tsg.table_name,
        tsg.full_table_name
    FROM {schema_name}.databricks_users u
    INNER JOIN table_select_grantees tsg ON u.id = tsg.grantee

    UNION

    -- Users in groups with SELECT on specific tables
    SELECT DISTINCT
        gtmf.member_id as user_id,
        gtmf.member_email as user_email,
        tsg.table_catalog,
        tsg.table_schema,
        tsg.table_name,
        tsg.full_table_name
    FROM {schema_name}.databricks_groups_to_members_flattened gtmf
    INNER JOIN table_select_grantees tsg ON gtmf.group_name = tsg.grantee
    WHERE gtmf.member_type = 'User'
),
-- Users who have all required permissions (catalog + schema + table access)
qualified_users AS (
    SELECT DISTINCT user_id, user_email FROM catalog_use_users
    INTERSECT
    SELECT DISTINCT user_id, user_email FROM schema_use_users
    INTERSECT
    SELECT DISTINCT user_id, user_email FROM table_select_users
)
-- Get table access for qualified users only
SELECT
    tsu.user_id,
    tsu.user_email,
    tsu.table_catalog,
    tsu.table_schema,
    tsu.table_name,
    tsu.full_table_name
FROM table_select_users tsu
INNER JOIN qualified_users qu ON tsu.user_id = qu.user_id AND tsu.user_email = qu.user_email
"""

qualified_table_access_df = spark.sql(qualified_table_access_query)
qualified_table_access_dataframe_name = "Qualified table access dataframe"
summarize_dataframe(qualified_table_access_df, qualified_table_access_dataframe_name)

# COMMAND ----------

# Write qualified table access to Delta table
write_df_to_delta(
    qualified_table_access_df,
    "databricks_users_default_tables_access",
    qualified_table_access_dataframe_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create aggregated table of users with access to default tables

# COMMAND ----------

# Create aggregated table from the detailed table access data
print("Creating aggregated users with access to default tables dataframe")

query_users_with_access_to_default_tables = f"""
-- Read from the persisted qualified table access data and aggregate by user
SELECT
    qta.user_id,
    qta.user_email,
    u.displayName as user_display,
    emp.department,
    emp.job_family_group,
    COUNT(DISTINCT qta.full_table_name) as table_count,
    COUNT(DISTINCT qta.table_schema) as schema_count
FROM {schema_name}.databricks_users_default_tables_access qta
INNER JOIN {schema_name}.databricks_users u ON qta.user_id = u.id
LEFT OUTER JOIN edw.silver.employee_hierarchy_active_vw emp ON emp.employee_email = qta.user_email
GROUP BY qta.user_id, qta.user_email, u.displayName, emp.department, emp.job_family_group
"""

users_with_access_to_default_tables_df = spark.sql(
    query_users_with_access_to_default_tables
)
users_with_access_to_default_tables_dataframe_name = (
    "Users with access to default tables dataframe"
)
summarize_dataframe(
    users_with_access_to_default_tables_df,
    users_with_access_to_default_tables_dataframe_name,
)

# COMMAND ----------

# Write users with access to default tables to Delta table
write_df_to_delta(
    users_with_access_to_default_tables_df,
    "databricks_users_with_access_to_default_tables",
    users_with_access_to_default_tables_dataframe_name,
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create table of users with Tableau Creator License
# COMMAND ----------

# Query to get all users with Tableau Creator License
tableau_creator_license_query = f"""
SELECT distinct
  user_email,
  emp.department,
  emp.job_family_group
FROM
  edw.silver.dim_tableau_user_groups
LEFT OUTER JOIN edw.silver.employee_hierarchy_active_vw emp ON emp.employee_email = user_email
WHERE
  site_role IN ('Creator', 'SiteAdministratorCreator')
"""
tableau_creator_license_df = spark.sql(tableau_creator_license_query)
tableau_creator_license_dataframe_name = "Tableau users with creator license dataframe"
summarize_dataframe(tableau_creator_license_df, tableau_creator_license_dataframe_name)

# COMMAND ----------

# Write tableau creator license DataFrame to Delta table
write_df_to_delta(
    tableau_creator_license_df,
    "tableau_users_with_creator_license",
    tableau_creator_license_dataframe_name,
)
# COMMAND ----------

# MAGIC %md
# MAGIC # Create table of users with Tableau Creator License and access to default catalog
# COMMAND ----------

# Query to get users with Tableau Creator License and access to default catalog
tableau_creator_license_and_default_catalog_access_query = f"""
SELECT
  user_email,
  department,
  job_family_group
FROM {schema_name}.tableau_users_with_creator_license
WHERE user_email IN (SELECT user_email FROM {schema_name}.databricks_users_with_access_to_default_tables)
"""
tableau_creator_license_and_default_catalog_access_df = spark.sql(
    tableau_creator_license_and_default_catalog_access_query
)
tableau_creator_license_and_default_catalog_access_dataframe_name = (
    "Tableau users with creator license and databricks default catalog access dataframe"
)
summarize_dataframe(
    tableau_creator_license_and_default_catalog_access_df,
    tableau_creator_license_and_default_catalog_access_dataframe_name,
)

# COMMAND ----------

# Write tableau creator license and default catalog access DataFrame to Delta table
write_df_to_delta(
    tableau_creator_license_and_default_catalog_access_df,
    "tableau_users_with_creator_license_and_databricks_default_catalog_access",
    tableau_creator_license_and_default_catalog_access_dataframe_name,
)
