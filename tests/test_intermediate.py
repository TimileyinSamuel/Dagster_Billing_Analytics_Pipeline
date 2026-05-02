INTERMEDIATE_TABLES = [
    "int_all_schedules",
    "int_employee_activity_daily",
    "int_employee_activity_weekly",
    "int_billable_employees_weekly",
    "int_location_metrics_weekly",
]


def test_intermediate_tables_exist(duckdb_con):
    tables = duckdb_con.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()

    table_names = {row[0] for row in tables}

    missing_tables = set(INTERMEDIATE_TABLES) - table_names

    assert not missing_tables, f"Missing intermediate tables: {missing_tables}"


def test_intermediate_tables_not_empty(duckdb_con):
    for table in INTERMEDIATE_TABLES:
        row_count = duckdb_con.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        assert row_count > 0, f"{table} should not be empty"