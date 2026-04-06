import dagster as dg

from billing_analytics_pipeline.assets import staging, intermediate, dimensions, facts
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource
from billing_analytics_pipeline.resources.duckdb_io_manager import duckdb_io_manager
from billing_analytics_pipeline.jobs.jobs import billing_pipeline_job
from billing_analytics_pipeline.schedules.schedules import billing_pipeline_schedule

staging_assets = dg.load_assets_from_modules([staging], group_name = "staging")
intermediate_assets = dg.load_assets_from_modules([intermediate], group_name = "intermediate")
dimension_assets = dg.load_assets_from_modules([dimensions], group_name = "dimensions")
fact_assets = dg.load_assets_from_modules([facts], group_name = "facts")

all_assets = (
    staging_assets
    + intermediate_assets
    + dimension_assets
    + fact_assets
)

defs = dg.Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(database="data/warehouse.duckdb"),
        "io_manager": duckdb_io_manager,
    },
    jobs = [billing_pipeline_job],
    schedules = [billing_pipeline_schedule],
)