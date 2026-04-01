import duckdb
import dagster as dg


class DuckDBResource(dg.ConfigurableResource):
    database: str

    def get_connection(self):
        return duckdb.connect(self.database)