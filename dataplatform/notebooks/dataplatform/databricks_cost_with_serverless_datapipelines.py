from pyspark.sql.types import MapType, StringType
from pyspark.sql import SparkSession
import json
import csv
import os
from pyspark.sql.functions import (
    col,
    collect_list,
    when,
    first,
    struct,
    get_json_object,
    expr,
    udf,
)

datapipelines_regional_service_principals = [
    "94266402-8dc8-44d7-8a4c-1439cb051aac",  # us-west-2
    "bea7b88b-63d5-4721-aa4e-2b33425560a8",  # eu-west-1
    "4c7141eb-d153-4670-b06e-186b94d53c03",  # ca-central-1
]


def parse_tags():
    tags = {}
    with open(
        "/Volumes/s3/dataplatform-deployed-artifacts/others/dataplatform/datapipelines/nodes_cost_attribution_tags.csv",
        mode="r",
    ) as file:
        csvFile = csv.reader(file)
        for lines in csvFile:
            tags[lines[0]] = lines[1]

    return tags


def main_cost_with_serverless_datapipelines(spark: SparkSession):
    tags_dict = parse_tags()

    # A UDF that will merge tags based on the datapipeline run name.
    def merge_tags(existing_tags, run_name):

        if not existing_tags or run_name in ["", None]:
            return existing_tags

        try:
            parsed_existing_tags = json.loads(existing_tags)
        except json.JSONDecodeError as e:
            print(f"Failed to parse tags: {existing_tags}. Error: {e}")
            return existing_tags

        for prefix, new_tags in tags_dict.items():
            if run_name.startswith(prefix):
                merged_tags = parsed_existing_tags.copy()
                new_tags = json.loads(new_tags)
                merged_tags.update(new_tags)
                merged_tags["UpdatedTags"] = "true"
                return json.dumps(merged_tags)

        return existing_tags

    merge_tags_udf = udf(merge_tags, StringType())

    df = spark.sql("SELECT * FROM billing.databricks")

    # Filter for datapipelines runs without tags
    filtered_df = df.filter(
        (
            get_json_object(col("tags"), "$.Creator").isin(
                datapipelines_regional_service_principals
            )
        )
        & ~(
            get_json_object(
                col("clustercustomtags"), "$.samsara:pooled-job:dataplatform-job-type"
            ).isNotNull()
            | get_json_object(
                col("clustercustomtags"), "$.samsara:dataplatform-job-type"
            ).isNotNull()
        )
    )

    # Create a mapping of job_id to run_name
    # We need to ask databricks why this happens.
    # Noticed for the same run there are multiple entries in the table.
    # Some of them have just job_id and no run_name.
    # Perhaps runname is generated after the run is started.
    job_runname_mapping = (
        filtered_df.filter(get_json_object(col("tags"), "$.RunName").isNotNull())
        .select(
            get_json_object(col("tags"), "$.JobId").alias("job_id"),
            get_json_object(col("tags"), "$.RunName").alias("run_name"),
        )
        .distinct()
    )

    # Join this mapping back to the original dataframe and add datapipeline run name column only if it is not null.
    result_df = df.join(
        job_runname_mapping,
        (get_json_object(col("tags"), "$.JobId") == col("job_id"))
        & (get_json_object(col("tags"), "$.JobId").isNotNull())
        & (get_json_object(col("tags"), "$.JobId") != "")
        & (col("job_id").isNotNull())
        & (col("job_id") != ""),
        "left",
    ).withColumn(
        "datapipeline_run_name",
        when(
            get_json_object(col("tags"), "$.JobId").isNotNull()
            & (get_json_object(col("tags"), "$.JobId") != "")
            & col("run_name").isNotNull(),
            col("run_name"),
        ).otherwise(""),
    )

    # Using the UDF to merge tags from nodes_cost_attribution_tags.csv, if datapipeline run name is not empty.
    df_with_merged_tags = result_df.withColumn(
        "clustercustomtags",
        merge_tags_udf(col("clustercustomtags"), col("datapipeline_run_name")),
    )

    # Drop the temporary columns created for the merge
    df_with_merged_tags = df_with_merged_tags.drop(
        "job_id", "run_name", "datapipeline_run_name"
    )

    # Write the result to a table.
    df_with_merged_tags.write.mode("overwrite").option(
        "mergeSchema", "true"
    ).saveAsTable("billing.databricks_cost_with_serverless_datapipelines")


if __name__ == "__main__":
    main_cost_with_serverless_datapipelines(spark)
