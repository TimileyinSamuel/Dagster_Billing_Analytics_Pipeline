def test_dim_accounts_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM dim_accounts"
    ).fetchone()[0]

    assert row_count > 0, "dim_accounts should not be empty"


def test_dim_accounts_account_id_not_null(duckdb_con):
    null_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM dim_accounts
        WHERE account_id IS NULL
    """).fetchone()[0]

    assert null_count == 0, "dim_accounts.account_id should not contain nulls"


def test_dim_accounts_account_id_unique(duckdb_con):
    duplicate_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT account_id
            FROM dim_accounts
            GROUP BY account_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    assert duplicate_count == 0, "dim_accounts.account_id should be unique"


def test_dim_locations_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM dim_locations"
    ).fetchone()[0]

    assert row_count > 0, "dim_locations should not be empty"


def test_dim_locations_account_id_relationship(duckdb_con):
    orphan_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM dim_locations l
        LEFT JOIN dim_accounts a
            ON l.account_id = a.account_id
        WHERE l.account_id IS NOT NULL
          AND a.account_id IS NULL
    """).fetchone()[0]

    assert orphan_count == 0, "dim_locations contains account_id values not found in dim_accounts"


def test_dim_memberships_not_empty(duckdb_con):
    row_count = duckdb_con.execute(
        "SELECT COUNT(*) FROM dim_memberships"
    ).fetchone()[0]

    assert row_count > 0, "dim_memberships should not be empty"


def test_dim_memberships_account_id_relationship(duckdb_con):
    orphan_count = duckdb_con.execute("""
        SELECT COUNT(*)
        FROM dim_memberships m
        LEFT JOIN dim_accounts a
            ON m.account_id = a.account_id
        WHERE m.account_id IS NOT NULL
          AND a.account_id IS NULL
    """).fetchone()[0]

    assert orphan_count == 0, "dim_memberships contains account_id values not found in dim_accounts"