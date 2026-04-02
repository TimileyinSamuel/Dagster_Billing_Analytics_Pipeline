import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource


@dg.asset(
    deps = ["stg_shifts", "stg_rests", "stg_user_contracts"]
)
def int_all_schedules(duckdb: DuckDBResource) -> None:
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


@dg.asset(
    deps = ["int_all_schedules"]
)
def int_employee_activity_daily(duckdb: DuckDBResource) -> None:
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


@dg.asset(
    deps = ["int_employee_activity_daily"]
)
def int_employee_activity_weekly(duckdb: DuckDBResource) -> None:
    query = """
    create or replace table int_employee_activity_weekly as
    with weekly_activity as (

    select
        user_contract_id,
        account_id,
        week_start,
        count(*) as active_days_in_week,
        1 as had_activity_in_week
    from {{ ref('int_employee_activity_daily') }}
    group by user_contract_id, account_id, week_start

)

    select *
    from weekly_activity
    """
    with duckdb.get_connection() as con:
        con.execute(query)


@dg.asset(
    deps = ["int_employee_activity_weekly", "stg_user_contracts", "stg_memberships"]
)
def int_billable_employees_weekly(duckdb: DuckDBResource) -> None:
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


@dg.asset(
    deps = ["int_billable_employees_weekly", "stg_locations"]
)
def int_location_metrics_weekly(duckdb: DuckDBResource) -> None:
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