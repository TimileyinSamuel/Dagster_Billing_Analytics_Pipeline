import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource
from billing_analytics_pipeline.utils.db_utils import get_row_count
from billing_analytics_pipeline.utils.metadata_utils import row_count_metadata

@dg.asset(
    deps = ["int_location_metrics_weekly"],
    description = "Applies pricing rules at the location-week level to calculate base price, additional employee costs, and total revenue."
)
def fct_location_revenue_weekly(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table fct_location_revenue_weekly as
    with pricing_inputs as (
    select
        account_id,
        location_id,
        week_start,
        billable_employee_count,
        location_size,
        is_billable_location
    from int_location_metrics_weekly

),

pricing_calculated as (

    select
        account_id,
        location_id,
        week_start,
        billable_employee_count,
        location_size,
        is_billable_location,

        case
            when is_billable_location = false then 0
            when location_size = 'micro' then 60
            when location_size = 'small' then 80
            when location_size = 'large' then 216
        end as base_price,

        case
            when is_billable_location = false then 0
            when billable_employee_count <= 6 then 0
            when billable_employee_count between 7 and 39 then billable_employee_count - 6
            else 33
        end as employees_in_7_39_band,

        case
            when is_billable_location = false then 0
            when billable_employee_count <= 39 then 0
            else billable_employee_count - 39
        end as employees_in_40_plus_band

    from pricing_inputs

),

final as (

    select
        account_id,
        location_id,
        week_start,
        billable_employee_count,
        location_size,
        is_billable_location,
        base_price,
        employees_in_7_39_band,
        employees_in_40_plus_band,
        (employees_in_7_39_band * 4) + (employees_in_40_plus_band * 2.4) as extra_employee_cost,
        base_price + ((employees_in_7_39_band * 4) + (employees_in_40_plus_band * 2.4)) as total_location_revenue
    from pricing_calculated

)

    select *
    from final
    """
    with duckdb.get_connection() as con:
        con.execute(query)
        # Get row count as materialized metadata
        num_rows = get_row_count(con, "fct_location_revenue_weekly")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )


@dg.asset(
    deps = ["fct_location_revenue_weekly"],
    description = "Aggregates location-level metrics to the account-week level, producing billable locations, employees, and expected revenue."
)
def fct_account_billing_weekly(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table fct_account_billing_weekly as
    with account_weekly as (

    select
        account_id,
        week_start,
        count(case when is_billable_location = true then 1 end) as billable_locations,
        sum(case when is_billable_location = true then billable_employee_count else 0 end) as billable_employees,
        sum(total_location_revenue) as expected_weekly_revenue
    from fct_location_revenue_weekly
    group by account_id, week_start

)
    select *
    from account_weekly
    """
    with duckdb.get_connection() as con:
        con.execute(query)
    # Get row count as materialized metadata
        num_rows = get_row_count(con, "fct_account_billing_weekly")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )