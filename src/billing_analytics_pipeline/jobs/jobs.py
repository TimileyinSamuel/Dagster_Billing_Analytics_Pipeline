import dagster as dg
from dagster import define_asset_job

assets_selection = dg.AssetSelection.groups("staging", "intermediate", "mart")

billing_pipeline_job = dg.define_asset_job(
    name = "billing_pipeline_job",
    selection = assets_selection
)