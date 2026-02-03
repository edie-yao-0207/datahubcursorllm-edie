from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

label_jsons_schema = StructType(
    [
        StructField("objects", ArrayType(IntegerType())),
        StructField(
            "classifications",
            ArrayType(
                StructType(
                    [
                        StructField("featureId", StringType()),
                        StructField("schemaId", StringType()),
                        StructField("scope", StringType()),
                        StructField("title", StringType()),
                        StructField("value", StringType()),
                        StructField(
                            "answers",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("featureId", StringType()),
                                        StructField("schemaId", StringType()),
                                        StructField("title", StringType()),
                                        StructField("value", StringType()),
                                        StructField("position", IntegerType()),
                                    ]
                                )
                            ),
                        ),
                        StructField(
                            "answer",
                            StructType(
                                [
                                    StructField("featureId", StringType()),
                                    StructField("schemaId", StringType()),
                                    StructField("title", StringType()),
                                    StructField("value", StringType()),
                                    StructField("position", IntegerType()),
                                ]
                            ),
                        ),
                    ]
                )
            ),
        ),
        StructField("relationships", ArrayType(IntegerType())),
    ]
)


@udf(MapType(StringType(), ArrayType(MapType(StringType(), StringType()))))
def parse_classifications(classifications, by):
    if classifications is None:
        return None
    o = {}
    for classification in classifications:
        if "answer" in classification and classification["answer"] is not None:
            o[classification[by]] = [
                {
                    "title": classification["answer"]["title"],
                    "value": classification["answer"]["value"],
                }
            ]
        elif "answers" in classification and classification["answers"] is not None:
            o[classification[by]] = [
                {"title": a["title"], "value": a["value"]}
                for a in classification["answers"]
            ]
        else:
            o[classification[by]] = None
    return o


projects = [
    "crash_instructions",
    "fleet-crash-model-development",
    "fleet-crash-production-labeling",
    "fleet-crash-customer-manually-generated",
    "fleet-crash-batch-model-development",
    "fleet-crash-evaluation",
    "fleet-crash-evaluation-backlog",
]
projects = [p.replace("-", "_") for p in projects]
TABLE = "datascience.raw_labelbox_crash_labels"

dfs = []
for project in projects:
    df = spark.table(f"labelbox.{project}")
    df = df.withColumn(
        "parsed_labels",
        F.transform(df.label_jsons, lambda x: F.from_json(x, label_jsons_schema)),
    )

    df = df.select(
        df.external_id,
        parse_classifications(
            df.parsed_labels[0].classifications, F.lit("title")
        ).alias("classifications_by_title"),
        parse_classifications(
            df.parsed_labels[0].classifications, F.lit("value")
        ).alias("classifications_by_value"),
        df.project_name,
        df.project_id,
        F.to_date(df.annotation_data.created_at).alias("label_date"),
        F.to_timestamp(df.annotation_data.created_at).alias("label_created_at"),
        F.to_timestamp(df.annotation_data.updated_at).alias("label_updated_at"),
        df.data_url,
        df.view_label.alias("label_url"),
        df.global_key,
        df.datarow_id,
        df.annotation_data.seconds_to_label.alias("seconds_to_label"),
        F.split(df.annotation_data.created_by, "@")[
            F.size(F.split(df.annotation_data.created_by, "@")) - 1
        ].alias("created_by_domain"),
        df.parsed_labels,
    )
    dfs.append(df)

to_write = reduce(DataFrame.unionAll, dfs)

to_write.write.partitionBy("label_date", "project_id").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(TABLE)
