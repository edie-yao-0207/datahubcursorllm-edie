from dagster import define_asset_job, load_assets_from_modules

from ..assets.examples import example_pyspark_assets_pipeline

pyspark_pipeline_assets = load_assets_from_modules([example_pyspark_assets_pipeline])


assets_with_databricks_step_launcher = define_asset_job(
    name="example_assets_with_databricks_step_launcher",
    description="Example asset job using Databricks step launcher compute engine",
    selection=pyspark_pipeline_assets,
    config=None,
    tags=None,
    metadata=None,
    partitions_def=None,
    executor_def=None,
    hooks=None,
)
