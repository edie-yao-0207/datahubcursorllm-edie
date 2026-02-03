# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT date,
# MAGIC        format_number(count(distinct sam_number), '#,###') as total_paid_customers
# MAGIC FROM   datamodel_core.dim_organizations
# MAGIC WHERE  date = (SELECT max(date)
# MAGIC                FROM   datamodel_core.dim_organizations) and is_paid_customer = true
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1 desc;

# COMMAND ----------
