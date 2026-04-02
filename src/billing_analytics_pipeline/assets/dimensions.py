import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource

@dg.asset(
    deps = ["stg_accounts"]
)
def dim_accounts(duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_accounts as
    select *
    from stg_accounts
    """
    with duckdb.get_connection() as con:
        con.execute(query)


@dg.asset(
    deps = ["stg_locations"]
)
def dim_locations(duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_locations as
    select *
    from stg_locations
    """
    with duckdb.get_connection() as con:
        con.execute(query)


@dg.asset(
    deps = ["stg_memberships"]
)
def dim_memberships(duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_memberships as
    select *
    from stg_memberships
    """
    with duckdb.get_connection() as con:
        con.execute(query)