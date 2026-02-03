# Databricks notebook source
# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import time

now = time.time()

print(time.strftime("%Y-%m-%d %H:%M:%S UTC", time.localtime(now)))

# COMMAND ----------

import boto3

aws_region_name = boto3.session.Session().region_name
print(aws_region_name)

# COMMAND ----------

# https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/infra/dataplatform/terraform/dataplatformconfig/databricksprovider.go

principal_by_aws_region_name = {
    "us-west-2": "790f7c2e-de70-456f-bca8-9f8276555a38",
    "eu-west-1": "bc1bfa85-1b1e-4990-b63c-1a49fb9053a0",
    "ca-central-1": "9ac897a0-c4dc-4d46-919f-9bb66ff1915f",
}

grantor_principal = None
if aws_region_name in principal_by_aws_region_name:
    grantor_principal = principal_by_aws_region_name[aws_region_name]

if grantor_principal is None:
    raise ValueError(
        f"Could not find grantor_principal for aws_region_name {aws_region_name}"
    )

print(grantor_principal)

# COMMAND ----------

df_groups = spark.sql("SHOW GROUPS").toPandas()
print(df_groups.shape)

# COMMAND ----------

df_default_counts = spark.sql(
    "SELECT grantee, count(distinct schema_name) as schemas_granted_count, count(0) as total_grant_count FROM system.information_schema.schema_privileges WHERE grantor = :grantor and catalog_name = 'default' and inherited_from = 'NONE' GROUP BY grantee ORDER BY grantee asc",
    args={"grantor": grantor_principal},
).toPandas()

print(df_default_counts.shape)

# COMMAND ----------

df_non_govramp_customer_data_counts = spark.sql(
    "SELECT grantee, count(distinct table_schema || '.' || table_name) as tables_granted_count, count(0) as total_grant_count FROM system.information_schema.table_privileges WHERE table_catalog = 'non_govramp_customer_data' and grantor = :grantor and inherited_from = 'NONE' GROUP BY grantee ORDER BY grantee asc",
    args={"grantor": grantor_principal},
).toPandas()

print(df_non_govramp_customer_data_counts.shape)

# COMMAND ----------

import pandas as pd


def merge(df_left, df_right, columns):
    df = pd.merge(
        left=df_left, right=df_right, how="left", left_on="name", right_on="grantee"
    )
    df = df[columns]
    df = df.fillna(0)
    return df


# COMMAND ----------

df_complete_default_counts = merge(
    df_groups, df_default_counts, ["name", "schemas_granted_count", "total_grant_count"]
)
print(df_complete_default_counts.shape)

# COMMAND ----------

df_complete_non_govramp_customer_data_counts = merge(
    df_groups,
    df_non_govramp_customer_data_counts,
    ["name", "tables_granted_count", "total_grant_count"],
)
print(df_complete_non_govramp_customer_data_counts.shape)

# COMMAND ----------

ssm_client = get_ssm_client("standard-read-parameters-ssm")
api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")

# COMMAND ----------

import datadog

datadog.initialize(api_key=api_key, app_key=app_key)

# COMMAND ----------


def send(rows, metric_name, catalog_name):
    for group_name, count in rows:
        datadog.api.Metric.send(
            metric=metric_name,
            points=[(now, count)],
            type="gauge",
            tags=[
                f"group:{group_name}",
                f"catalog:{catalog_name}",
                f"region:{aws_region_name}",
            ],
        )

    print(f"{catalog_name} {len(rows)} {metric_name} sent")


# COMMAND ----------

send(
    rows=df_complete_default_counts[["name", "schemas_granted_count"]].values,
    metric_name="databricks.grants.groups.num_distinct_schemas_granted",
    catalog_name="default",
)

# COMMAND ----------

send(
    rows=df_complete_default_counts[["name", "total_grant_count"]].values,
    metric_name="databricks.grants.groups.num_schema_grants",
    catalog_name="default",
)

# COMMAND ----------

send(
    rows=df_complete_non_govramp_customer_data_counts[
        ["name", "tables_granted_count"]
    ].values,
    metric_name="databricks.grants.groups.num_distinct_tables_granted",
    catalog_name="non_govramp_customer_data",
)

# COMMAND ----------

send(
    rows=df_complete_non_govramp_customer_data_counts[
        ["name", "total_grant_count"]
    ].values,
    metric_name="databricks.grants.groups.num_table_grants",
    catalog_name="non_govramp_customer_data",
)
