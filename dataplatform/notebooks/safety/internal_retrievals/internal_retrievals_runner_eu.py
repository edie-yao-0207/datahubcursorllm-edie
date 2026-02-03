# MAGIC %run ./internal_retrievals

# COMMAND ----------

# Fetch 600 trips longer than 10 minutes after July 1st, 2022 12:00am PT from a random set of devices with CMs
df_random_device_trips = spark.sql(
    """
select
  t.org_id,
  t.device_id,
  max(t.start_ms) as start_ms
from
  productsdb.devices d
  left join trips2db_shards.trips t on d.org_id = t.org_id
  and d.id = t.device_id
  and t.version = 101
where
  d.camera_product_id in (43, 44)
  and t.start_ms > 1656658800000
  and t.end_ms - t.start_ms > 600000
group by
  t.org_id,
  t.device_id
order by
  rand()
limit
  0
"""
)

trip_start_offset_ms = 300000  # 5 minutes
retrieval_length_ms = 10000  # 10 seconds
test_retrievals = []

for trip in df_random_device_trips.collect():
    org_id = trip["org_id"]
    device_id = trip["device_id"]
    start_ms = trip["start_ms"] + trip_start_offset_ms
    end_ms = start_ms + retrieval_length_ms
    test_retrievals.append(
        InternalRetrievalRequest(
            org_id,
            device_id,
            start_ms,
            end_ms,
        )
    )

main(test_retrievals)
