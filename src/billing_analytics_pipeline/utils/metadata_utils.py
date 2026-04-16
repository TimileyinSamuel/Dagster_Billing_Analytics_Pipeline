"""
metadata_utils.py

This module contains helper functions for building Dagster materialization metadata.

It is responsible for:
- Formatting runtime values (e.g., row counts) into Dagster MetadataValue objects
- Standardizing how metadata is attached to asset materializations

These utilities help ensure consistency and readability across all assets,
while keeping metadata logic separate from database and transformation logic.
"""

import dagster as dg


def row_count_metadata(num_rows: int) -> dict:
    """
    Build Dagster materialization metadata for row count.

    Args:
        num_rows (int): Number of rows in the table

    Returns:
        dict: Metadata dictionary for MaterializeResult
    """
    return {
        "num_rows": dg.MetadataValue.int(num_rows),
    }