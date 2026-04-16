import dagster as dg
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource
from billing_analytics_pipeline.utils.db_utils import get_row_count
from billing_analytics_pipeline.utils.metadata_utils import row_count_metadata

@dg.asset(
    deps = ["stg_accounts"],
    description = "Curated account dimension with business-ready attributes for reporting and aggregation."
)
def dim_accounts(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table dim_accounts as
    select *
    from stg_accounts
    """
    with duckdb.get_connection() as con:
        con.execute(query)
    # Get row count as materialized metadata
        num_rows = get_row_count(con, "dim_accounts")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )


@dg.asset(
    deps = ["stg_locations"],
    description = "Location dimension enriched with account relationships, attributes, and billing-related flags."
)
def dim_locations(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table dim_locations as
    select *
    from stg_locations
    """
    with duckdb.get_connection() as con:
        con.execute(query)
    # Get row count as materialized metadata
        num_rows = get_row_count(con, "dim_locations")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )


@dg.asset(
    deps = ["stg_memberships"],
    description = "Employee dimension representing unique memberships with associated account and role information."
)
def dim_memberships(context: dg.AssetExecutionContext, duckdb: DuckDBResource) -> dg.MaterializeResult:
    query = """
    create or replace table dim_memberships as
    select *
    from stg_memberships
    """
    with duckdb.get_connection() as con:
        con.execute(query)
    # Get row count as materialized metadata
        num_rows = get_row_count(con, "dim_memberships")
    
    return dg.MaterializeResult(
        metadata = row_count_metadata(num_rows)
    )