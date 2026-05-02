FACT_TABLES = [
    "fct_location_revenue_weekly",
    "fct_account_billing_weekly",
]


def test_fact_tables_exist(duckdb_con):
    tables = duckdb_con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()

    table_names = {row[0] for row in tables}

    missing_tables = set(FACT_TABLES) - table_names

    assert not missing_tables, f"Missing fact tables: {missing_tables}"


def test_fact_tables_not_empty(duckdb_con):
    for table in FACT_TABLES:
        row_count = duckdb_con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        assert row_count > 0, f"{table} should not be empty"