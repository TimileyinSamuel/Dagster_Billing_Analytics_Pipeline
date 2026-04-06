import dagster as dg
from billing_analytics_pipeline.jobs.jobs import billing_pipeline_job

# a schedule to run this pipeline everyday at 2am from Monday to Friday
billing_pipeline_schedule = dg.ScheduleDefinition(
    job = billing_pipeline_job,
    cron_schedule = "0 2 * * 1-5"
)