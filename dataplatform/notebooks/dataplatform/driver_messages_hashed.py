# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import hashlib

table_name = "messagesdb_hashed.messages_hashed"

# This databricks secret can be accessed by dataplatform
# these are not managed in code and are managed via databricks-cli
# dataplatform team has access to this secret.
# This secret is used to hash the driver ids and user ids in the messages table.
# This is done to protect the privacy of the drivers and users.
# Change to salt will change the hash of the driver ids and user ids and downstream processs
# will need to know about the change. The current consumer is the Owl chat AI team.
salt = dbutils.secrets.get(scope="dataplatform-secrets", key="rds_hashing_salt_key")

if salt == None or salt == "":
    raise Exception(
        "Secret does not exist with scope: dataplatform-secrets and key: rds_hashing_salt_key"
    )


def hash_with_salt(value):
    # Hash a value with the salt using SHA-256.
    # Returns null if the input value is null, preserving null values.

    if value is None:
        return None

    # Convert to string and hash with salt
    value_str = str(value)
    salted_value = salt + value_str
    return hashlib.sha256(salted_value.encode("utf-8")).hexdigest()


# Register the UDF to hash the driver ids and user ids.
hash_udf = F.udf(hash_with_salt, StringType())


def main():
    # Read all the messages from the messages table
    df = spark.sql("select * from  messagesdb_shards.messages")

    # Read the orgs that have opted out of using data for training
    filtered_orgs = spark.sql(
        "select org_id from dojo.ml_blocklisted_org_ids"
    ).collect()
    filtered_org_ids = [row.org_id for row in filtered_orgs]
    # Filter out the orgs that have opted out of using data for training
    df = df.filter(~df["org_id"].isin(filtered_org_ids))

    # Hash the driver ids and user_id with proper null handling
    # Using UDF to avoid salt exposure in execution plans
    df = (
        df.withColumn(
            "driver_message_channel_driver_id_hashed",
            hash_udf(F.col("driver_message_channel_driver_id")),
        )
        .withColumn("sender_driver_id_hashed", hash_udf(F.col("sender_driver_id")))
        .withColumn("sender_user_id_hashed", hash_udf(F.col("sender_user_id")))
    )
    # Drop the original columns
    df = df.drop(
        "driver_message_channel_driver_id", "sender_driver_id", "sender_user_id"
    )

    # Write the hashed messages to the s3 path
    df.write.mode("overwrite").saveAsTable(table_name)


main()

# COMMAND ----------
