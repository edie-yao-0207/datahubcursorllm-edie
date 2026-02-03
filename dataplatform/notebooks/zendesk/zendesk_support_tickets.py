spark.read.format("bigquery").option("table", "zd.support_tickets").load().write.format(
    "delta"
).mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "zendesk.support_tickets"
)
