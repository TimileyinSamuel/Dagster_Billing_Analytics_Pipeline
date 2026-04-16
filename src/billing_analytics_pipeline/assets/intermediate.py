import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource
from billing_analytics_pipeline.utils.db_utils import get_row_count
from billing_analytics_pipeline.utils.metadata_utils import row_count_metadata

@dg.asset(
    deps = ["stg_shifts", "stg_rests", "stg_user_contracts"],
    description = "Combines shifts and rests into a unified schedule dataset representing all qualifying employee activity events."
)
def int_all_schedules(context: dg.AssetExecutionContext,duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table int_all_schedules as
    with valid_shifts as (

    select
        cast(s.shift_id as varchar) as event_id,
        'shift' as event_type,
        s.user_contract_id,
        s.account_id,
        s.starts_at_utc,
        s.ends_at_utc,
        cast(s.starts_at_utc as date) as activity_date,
        cast(date_trunc('week', s.starts_at_utc) as date) as week_start
    from stg_shifts s
    inner join stg_user_contracts c
        on s.user_contract_id = c.user_contract_id

),

valid_rests as (

    select
        cast(r.rest_id as varchar) as event_id,
        'rest' as event_type,
        r.user_contract_id,
        r.account_id,
        r.starts_at_utc,
        r.ends_at_utc,
        cast(r.starts_at_utc as date) as activity_date,
        cast(date_trunc('week', r.starts_at_utc) as date) as week_start
    from stg_rests r
    inner join stg_user_contracts c
        on r.user_contract_id = c.user_contract_id

)

    select * from valid_shifts
    union all
    select * from valid_rests
    """
    with duckdb.get_connection() as con:
        con.execute(query)

    # Get row count as materialized metadata
        num_rows = get_row_count(con, "int_all_schedules")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )


@dg.asset(
    deps = ["int_all_schedules"],
    description = "Aggregates schedule events to one row per contract per day, creating a daily activity signal and removing duplicate events."
)
def int_employee_activity_daily(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table int_employee_activity_daily as
    with daily_activity as (

    select
        user_contract_id,
        account_id,
        activity_date,
        week_start,
        count(*) as schedule_count,
        1 as had_activity
    from int_all_schedules
    group by user_contract_id, account_id, activity_date, week_start

)
    select *
    from daily_activity
    """
    with duckdb.get_connection() as con:
        con.execute(query)

    # Get row count before and after transformation as materialized metadata
        row_before_transformation = con.execute("select count(*) from int_all_schedules").fetchone()
        num_rows_before_transformation = row_before_transformation[0]
        rows_after_transformation = con.execute("select count(*) from int_employee_activity_daily").fetchone()
        num_of_rows_after_transformation = rows_after_transformation[0]

    return dg.MaterializeResult(
        metadata = {
            "Number_of_rows_before_transformation": dg.MetadataValue.int(num_rows_before_transformation),
            "Number of rows_after_transformation": dg.MetadataValue.int(num_of_rows_after_transformation)
        }
    )


@dg.asset(
    deps = ["int_employee_activity_daily"],
    description = "Aggregates daily activity into weekly signals, producing one row per contract per week with activity metrics."
)
def int_employee_activity_weekly(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table int_employee_activity_weekly as
    with weekly_activity as (

    select
        user_contract_id,
        account_id,
        week_start,
        count(*) as active_days_in_week,
        1 as had_activity_in_week
    from int_employee_activity_daily
    group by user_contract_id, account_id, week_start

)

    select *
    from weekly_activity
    """
    with duckdb.get_connection() as con:
        con.execute(query)

    # Get row count before and after transformation as as materialized metadata
        row_before_transformation = con.execute("select count(*) from int_employee_activity_daily").fetchone()
        num_rows_before_transformation = row_before_transformation[0]
        rows_after_transformation = con.execute("select count(*) from int_employee_activity_weekly").fetchone()
        num_of_rows_after_transformation = rows_after_transformation[0]

    return dg.MaterializeResult(
        metadata = {
            "Number_of_rows_before_transformation": dg.MetadataValue.int(num_rows_before_transformation),
            "Number of rows_after_transformation": dg.MetadataValue.int(num_of_rows_after_transformation)
        }
    )


@dg.asset(
    deps = ["int_employee_activity_weekly", "stg_user_contracts", "stg_memberships"],
    description = "Converts contract-level activity into employee-level billability, ensuring one record per employee, location, and week."
)
def int_billable_employees_weekly(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table int_billable_employees_weekly as 
    with contract_activity as (
    select
        a.account_id,
        a.week_start,
        c.membership_id,
        a.user_contract_id,
        c.location_id,
        a.active_days_in_week
    from int_employee_activity_weekly a
    inner join stg_user_contracts c
        on a.user_contract_id = c.user_contract_id
       and a.account_id = c.account_id
    inner join stg_memberships m
        on c.membership_id = m.membership_id
       and c.account_id = m.account_id

),

deduplicated as (

    select
        account_id,
        week_start,
        membership_id,
        location_id,
        min(user_contract_id) as user_contract_id,
        max(active_days_in_week) as active_days_in_week,
        1 as is_billable_employee
    from contract_activity
    group by account_id, week_start, membership_id, location_id

)
    select *
    from deduplicated
    """
    with duckdb.get_connection() as con:
        con.execute(query)

    # Get row count as materialized metadata
        num_rows = get_row_count(con, "int_billable_employees_weekly")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )


@dg.asset(
    deps = ["int_billable_employees_weekly", "stg_locations"],
    description = "Aggregates billable employees at the location-week level and derives location size and billable status."
)
def int_location_metrics_weekly(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table int_location_metrics_weekly as
    with location_employee_counts as (

    select
        account_id,
        location_id,
        week_start,
        count(distinct membership_id) as billable_employee_count
    from int_billable_employees_weekly
    group by account_id, location_id, week_start

),

location_enriched as (

    select
        lec.account_id,
        lec.location_id,
        lec.week_start,
        lec.billable_employee_count,
        l.is_archived,
        case
            when l.is_archived = false then true
            else false
        end as is_billable_location,
        case
            when lec.billable_employee_count between 0 and 5 then 'micro'
            when lec.billable_employee_count between 6 and 39 then 'small'
            else 'large'
        end as location_size
    from location_employee_counts lec
    inner join stg_locations l
        on lec.location_id = l.location_id
       and lec.account_id = l.account_id

)

    select *
    from location_enriched
    """
    with duckdb.get_connection() as con:
        con.execute(query) 

    # Get row count as materialized metadata
        num_rows = get_row_count(con, "int_location_metrics_weekly")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )