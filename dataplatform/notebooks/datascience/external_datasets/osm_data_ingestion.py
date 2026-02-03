# Databricks notebook source
# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql.functions import col, collect_list, explode, size, struct, udf
from pyspark.sql.types import ArrayType, MapType, StringType


def decode_tags(tags):
    """
    decodes utf-8 encoded tags as part of the parquetized osm table.
    """
    decode_tag = lambda tag: {
        "key": tag.key.decode("utf-8"),
        "value": tag.value.decode("utf-8"),
    }
    return [decode_tag(x) for x in tags]


decode_tags_udf = udf(decode_tags, ArrayType(MapType(StringType(), StringType())))

# COMMAND ----------


def ingest_osm_way_data(sam_version_id, region):
    """
    converts the osm way and node parquet files into a single databricks table for a particular version_id and region.
    """
    print(f"Extracting for {region}, {sam_version_id} from:")
    mnt_prefix = os.path.join(
        "/mnt/", os.path.join(OSM_S3.BUCKET, OSM_S3.make_s3_key(sam_version_id, region))
    )
    print(mnt_prefix + ".way.parquet")

    ways_sdf = (
        sqlContext.read.parquet(mnt_prefix + ".way.parquet")
        .withColumnRenamed("id", "way_id")
        .withColumnRenamed("version", "osm_version")
        .drop("timestamp", "changeset", "uid", "user_sid")
    )
    ways_sdf_with_sam_version = ways_sdf.withColumn(
        "sam_osm_version", F.lit(sam_version_id)
    ).withColumn("region", F.lit(region))

    ways_sdf_with_sam_version = ways_sdf_with_sam_version.withColumn(
        "tags", decode_tags_udf(col("tags"))
    )

    # Drop partition if it exists so we can reinsert
    db_name = "datascience"
    table_name = "raw_osm_ways"

    if spark._jsparkSession.catalog().tableExists(db_name, table_name):
        ways_sdf_with_sam_version.write.format("delta").mode("overwrite").partitionBy(
            "sam_osm_version", "region"
        ).option(
            "replaceWhere",
            f"sam_osm_version = '{sam_version_id}' AND region = '{region}'",
        ).saveAsTable(
            f"{db_name}.{table_name}"
        )
    else:
        ways_sdf_with_sam_version.write.format("delta").partitionBy(
            "sam_osm_version", "region"
        ).saveAsTable(f"{db_name}.{table_name}")

    print(f"Data written to {db_name}.{table_name}")


# COMMAND ----------


def ingest_osm_node_data(sam_version_id, region):
    """
    converts the osm way and node parquet files into a single databricks table for a particular version_id and region.
    """
    print(f"Extracting for {region}, {sam_version_id} from:")
    mnt_prefix = os.path.join(
        "/mnt/", os.path.join(OSM_S3.BUCKET, OSM_S3.make_s3_key(sam_version_id, region))
    )
    print(mnt_prefix + ".node.parquet")

    nodes_sdf = (
        sqlContext.read.parquet(mnt_prefix + ".node.parquet")
        .withColumnRenamed("id", "node_id")
        .withColumnRenamed("version", "osm_version")
        .drop("timestamp", "changeset", "uid", "user_sid")
    )
    nodes_sdf_with_sam_version = nodes_sdf.withColumn(
        "sam_osm_version", F.lit(sam_version_id)
    ).withColumn("region", F.lit(region))

    nodes_sdf_with_sam_version = nodes_sdf_with_sam_version.withColumn(
        "tags", decode_tags_udf(col("tags"))
    )

    # Drop partition if it exists so we can reinsert
    db_name = "datascience"
    table_name = "raw_osm_nodes"
    if spark._jsparkSession.catalog().tableExists(db_name, table_name):
        nodes_sdf_with_sam_version.write.format("delta").mode("overwrite").partitionBy(
            "sam_osm_version", "region"
        ).option(
            "replaceWhere",
            f"sam_osm_version = '{sam_version_id}' AND region = '{region}'",
        ).saveAsTable(
            f"{db_name}.{table_name}"
        )
    else:
        nodes_sdf_with_sam_version.write.format("delta").partitionBy(
            "sam_osm_version", "region"
        ).saveAsTable(f"{db_name}.{table_name}")

    print(f"Data written to {db_name}.{table_name}")


# COMMAND ----------

sam_version_id = dbutils.widgets.get("sam_version_id")
region = dbutils.widgets.get("region")
ingest_osm_way_data(sam_version_id, region)
ingest_osm_node_data(sam_version_id, region)
