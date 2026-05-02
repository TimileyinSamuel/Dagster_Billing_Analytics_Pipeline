import pytest
from billing_analytics_pipeline.resources.duckdb_resource import DuckDBResource


@pytest.fixture
def duckdb_con():
    duckdb = DuckDBResource(database="data/warehouse.duckdb")

    with duckdb.get_connection() as con:
        yield con