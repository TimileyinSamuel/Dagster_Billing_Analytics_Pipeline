def test_stg_accounts_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM stg_accounts"
    ).fetchone()[0]

    assert row_count > 0, "stg_accounts should not be empty"


def test_stg_accounts_account_id_not_null(duckdb_con):
    null_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM stg_accounts
        WHERE account_id IS NULL
    """).fetchone()[0]

    assert null_count == 0, "stg_accounts.account_id should not contain nulls"


def test_stg_accounts_account_id_unique(duckdb_con):
    duplicate_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT account_id
            FROM stg_accounts
            GROUP BY account_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    assert duplicate_count == 0, "stg_accounts.account_id should be unique"


def test_stg_locations_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM stg_locations"
    ).fetchone()[0]

    assert row_count > 0, "stg_locations should not be empty"


def test_stg_memberships_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM stg_memberships"
    ).fetchone()[0]

    assert row_count > 0, "stg_memberships should not be empty"