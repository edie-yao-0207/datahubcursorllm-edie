import boto3

s3_bucket = "s3://samsara-eu-benchmarking-metrics"


def main():
    if boto3.Session().region_name != "eu-west-1":
        # don't run if not in EU region
        return

    spark.sql(
        """
    select
      org_id,
      distance_driven_miles_per_vehicle,
      trip_length,
      unique_active_vehicles,
      percent_passenger,
      percent_trips_city
    from
      dataproducts.org_attributes
  """
    ).coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(
        f"{s3_bucket}/org_attributes_eu"
    )


main()
