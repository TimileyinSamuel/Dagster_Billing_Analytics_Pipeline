"""
db_utils.py

This module contains helper functions for interacting with the database.

It is responsible for:
- Executing common database queries
- Extracting values from query results (e.g., row counts)

These utilities are designed to be reused across multiple assets
(staging, intermediate, dimensions, facts) to avoid duplicating
database-related logic inside each asset definition.
"""

def get_row_count(con, table_name: str) -> int:
    """
    Return the number of rows in a table.

    Args:
        con: Active DuckDB connection
        table_name (str): Name of the table to count rows from

    Returns:
        int: Number of rows in the table
    """
    result = con.execute(f"select count(*) from {table_name}").fetchone()
    return result[0]