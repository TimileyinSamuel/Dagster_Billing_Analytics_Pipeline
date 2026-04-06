import dagster as dg
from dagster import define_asset_job

all_assets_selection = dg.AssetSelection.all()

billing_pipeline_job = dg.define_asset_job(
    name = "billing_pipeline_job",
    selection = all_assets_selection
)