spark.read.format("bigquery").option(
    "table", "backend.devices_latest"
).load().write.format("delta").option("mergeSchema", "true").saveAsTable(
    "bigquery.backend_devices_latest", mode="overwrite"
)
