# MAGIC %run /backend/safety/internal_retrievals/internal_retrievals

# COMMAND ----------


from datetime import datetime, timedelta

RUN_DATE = str(
    (datetime.utcnow() - timedelta(days=1)).date()
)  # coordinated with sampling runs
df_requests = spark.table("datascience.daily_video_requests")
df_requests = df_requests.where(df_requests.date == RUN_DATE).select(
    df_requests.org_id, df_requests.device_id, df_requests.start_ms, df_requests.end_ms
)
requests = df_requests.collect()

# Limit to 3k requests to prevent overloading prod cells
assert len(requests) <= 3000, f"More than 3,000 requests for {RUN_DATE}"
# Limit requests to 90 seconds in length
assert all(
    [(r["end_ms"] - r["start_ms"]) <= 90_000 for r in requests]
), f"Request longer than 90 seconds {RUN_DATE}"

retrievals = []

for request in requests:
    org_id = request["org_id"]
    device_id = request["device_id"]
    start_ms = request["start_ms"]
    end_ms = request["end_ms"]
    retrievals.append(
        InternalRetrievalRequest(
            org_id,
            device_id,
            start_ms,
            end_ms,
        )
    )

main(retrievals)
