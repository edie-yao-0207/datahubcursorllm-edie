# Databricks notebook source
# MAGIC %md
# MAGIC ## Readme
# MAGIC This is the backend that will power the OEM Tableau dashboard. This populates the `dataprep_telematics.oem_active_sources` table on a daily basis with the latest status.

# COMMAND ----------

# DBTITLE 1,Create OEM Lookup Table dataprep_telematics.oem_lu
create_oem_lu_query = """
    CREATE TABLE IF NOT EXISTS dataprep_telematics.oem_lu (
      oem_type double,
      oem_name string
    )
"""

# Modify this query for future integrations.
insert_oem_lu_values_query = """
  INSERT INTO dataprep_telematics.oem_lu VALUES
    (0, "John Deere"),
    (2, "Caterpillar"),
    (3, "Ford"),
    (4, "Navistar"),
    (5, "Volvo Mack"),
    (6, "FCA"),
    (7, "Thermoking"),
    (8, "Komatsu"),
    (9, "GM"),
    (12, "Volvo CareTrack"),
    (13, "Case SiteWatch"),
    (14, "Vermeer"),
    (15, "Bobcat Machine IQ"),
    (16, "Carrier eSolutions"),
    (17, "Stellantis"),
    (18, "Tesla"),
    (19, "CAT AEMP 2.0"),
    (20, "Hino"),
    (21, "Drov"),
    (22, "Continental"),
    (23, "Rivian"),
    (24, "Liebherr"),
    (25, "SKF"),
    (26, "Carrier Eu");
"""


spark.sql(create_oem_lu_query)
spark.sql(insert_oem_lu_values_query)

# COMMAND ----------

from pyspark.sql.functions import *

oem_sources = spark.table("oemdb_shards.oem_sources")
customer_metadata = spark.table("dataprep.customer_metadata")
organizations = spark.table("clouddb.organizations")
oem_lu = spark.table("dataprep_telematics.oem_lu")

oem_active_sources_tmp = (
    oem_sources.join(
        customer_metadata, oem_sources.org_id == customer_metadata.org_id, "left"
    )
    .join(organizations, oem_sources.org_id == organizations.id, "left")
    .join(oem_lu, oem_sources.oem_type == oem_lu.oem_type, "left")
    .select(
        oem_sources.org_id,
        organizations.name,
        oem_lu.oem_name,
        oem_sources.source_id,
        oem_sources.device_id,
        oem_sources.created_at,
        oem_sources.activated_at,
        oem_sources.is_activated,
        organizations.internal_type,
        organizations.internal_type.alias("internal_org"),
        customer_metadata.sfdc_id,
    )
)

oem_active_sources_tmp.write.mode("overwrite").saveAsTable(
    "dataprep_telematics.oem_active_sources"
)
