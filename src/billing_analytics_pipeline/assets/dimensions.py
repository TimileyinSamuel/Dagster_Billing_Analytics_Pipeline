import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource

@dg.asset(
    deps = ["stg_accounts"],
    description = "Curated account dimension with business-ready attributes for reporting and aggregation."
)
def dim_accounts(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_accounts as
    select *
    from stg_accounts
    """
    with duckdb.get_connection() as con:
        con.execute(query)


@dg.asset(
    deps = ["stg_locations"],
    description = "Location dimension enriched with account relationships, attributes, and billing-related flags."
)
def dim_locations(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_locations as
    select *
    from stg_locations
    """
    with duckdb.get_connection() as con:
        con.execute(query)


@dg.asset(
    deps = ["stg_memberships"],
    description = "Employee dimension representing unique memberships with associated account and role information."
)
def dim_memberships(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> None:
    query = """
    create or replace table dim_memberships as
    select *
    from stg_memberships
    """
    with duckdb.get_connection() as con:
        con.execute(query)