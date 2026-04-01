from dagster_duckdb_pandas import DuckDBPandasIOManager

duckdb_io_manager = DuckDBPandasIOManager(
    database="data/warehouse.duckdb",
    schema="main",
)