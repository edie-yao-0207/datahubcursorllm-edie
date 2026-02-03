"""Simple DQ example - DataWeb-style decorator."""

from dagster import OpExecutionContext
from ml.common import NonEmptyDQCheck, NonNullDQCheck, Operator, SQLDQCheck, table_op
from pyspark.sql import SparkSession


@table_op(
    dq_checks=[
        NonEmptyDQCheck(name="nonempty"),
        NonNullDQCheck(name="nonnull", columns=["org_id"]),
        SQLDQCheck(
            name="sql_row_count_check",
            sql=("SELECT COUNT(*) as observed_value FROM df "),
            expected_value=100,
            operator=Operator.eq,
        ),
    ],
    database="dataplatform_dev",
    table="test_table_for_ml_project",
    owner="DataPlatform",
)
def simple_dq_example_op(context: OpExecutionContext):
    """Simple example - just return the dataframe!"""
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    return spark.sql(
        "SELECT org_id, org_name " "FROM datamodel_core.dim_organizations LIMIT 100"
    )
