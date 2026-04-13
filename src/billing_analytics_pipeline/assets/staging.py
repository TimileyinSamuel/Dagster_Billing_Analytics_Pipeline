import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource

@dg.asset(
        description = "Standardized account data with cleaned fields and unique account identifiers."
)
def stg_accounts(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_accounts as
    select
        cast(id as varchar) as account_id,
        cast(name as varchar) as account_name,
        cast(created_at as timestamp) as created_at,
        cast(time_zone as varchar) as time_zone,
        cast(country as varchar) as country
    from raw_accounts
    """
    with duckdb.get_connection() as con:
        con.execute(query)
    

@dg.asset(
        description = "Cleaned location data linked to accounts, including standardized archival status and location attributes."
)
def stg_locations(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_locations as
    select
    cast(id as varchar) as location_id,
    cast(name as varchar) as location_name,
    cast(account_id as varchar) as account_id,
    case
        when lower(trim(cast(archived as varchar))) in ('true', '1', 'yes') then true
        when lower(trim(cast(archived as varchar))) in ('false', '0', 'no') then false
        else null
    end as is_archived,
    cast(city as varchar) as city,
    cast(zipcode as varchar) as zipcode,
    cast(country as varchar) as country
    from raw_locations
    """
    with duckdb.get_connection() as con:
        con.execute(query)

@dg.asset(
        description = "Standardized employee membership data with account relationships and active status."
)
def stg_memberships(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_memberships as
    select
    cast(id as varchar) as membership_id,
    cast(user_id as varchar) as user_id,
    cast(account_id as varchar) as account_id,
    case
        when lower(trim(cast(active as varchar))) in ('true', '1', 'yes') then true
        when lower(trim(cast(active as varchar))) in ('false', '0', 'no') then false
        else null
    end as is_active,
    lower(trim(cast(role as varchar))) as role,
    cast(firstname as varchar) as first_name,
    cast(lastname as varchar) as last_name
    from raw_memberships
    """
    with duckdb.get_connection() as con:
        con.execute(query)

@dg.asset(
        description = "Normalized rest period data representing non-working but billable activity events."
)
def stg_rests(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_rests as
    select
    cast(id as varchar) as rest_id,
    cast(user_contract_id as varchar) as user_contract_id,
    cast(account_id as varchar) as account_id,
    cast(starts_at as timestamp) as starts_at_utc,
    cast(ends_at as timestamp) as ends_at_utc,
    lower(trim(cast(rest_type as varchar))) as rest_type
    from raw_rests
    """
    with duckdb.get_connection() as con:
        con.execute(query)

@dg.asset(
        description = "Normalized shift schedule data with validated timestamps and contract references."
)
def stg_shifts(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_shifts as
    select
    cast(id as varchar) as shift_id,
    cast(user_contract_id as varchar) as user_contract_id,
    cast(account_id as varchar) as account_id,
    cast(starts_at as timestamp) as starts_at_utc,
    cast(ends_at as timestamp) as ends_at_utc,
    lower(trim(cast(planification_type as varchar))) as planification_type
    from raw_shifts
    """
    with duckdb.get_connection() as con:
        con.execute(query)

@dg.asset(
        description = "Cleaned contract data linking employees to locations, with standardized dates and working hours."
)
def stg_user_contracts(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table stg_user_contracts as
    select
    cast(id as varchar) as user_contract_id,
    cast(membership_id as varchar) as membership_id,
    cast(location_id as varchar) as location_id,
    cast(account_id as varchar) as account_id,
    case
        when regexp_matches(trim(cast(contract_start as varchar)), '^\d{4}-\d{2}-\d{2}$')
            then cast(trim(cast(contract_start as varchar)) as date)
        when regexp_matches(trim(cast(contract_start as varchar)), '^\d{2}/\d{2}/\d{4}$')
            then strptime(trim(cast(contract_start as varchar)), '%d/%m/%Y')::date
        else null
    end as contract_start,
    case
        when contract_end is null then null
        when regexp_matches(trim(cast(contract_end as varchar)), '^\d{4}-\d{2}-\d{2}$')
            then cast(trim(cast(contract_end as varchar)) as date)
        when regexp_matches(trim(cast(contract_end as varchar)), '^\d{2}/\d{2}/\d{4}$')
            then strptime(trim(cast(contract_end as varchar)), '%d/%m/%Y')::date
        else null
    end as contract_end,
    lower(trim(cast(contract_type as varchar))) as contract_type,
    case
        when contract_time is null then null
        else cast(
            replace(
                replace(lower(trim(cast(contract_time as varchar))), 'h', ''),
                ',', '.'
            ) as double
        )
    end as contract_time
    from raw_user_contracts
    """
    with duckdb.get_connection() as con:
        con.execute(query)