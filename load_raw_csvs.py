from pathlib import Path
import duckdb

DB_PATH = "data/warehouse.duckdb"
RAW_DIR = Path("data/raw")

FILES = {
    "raw_accounts": "accounts.csv",
    "raw_locations": "locations.csv",
    "raw_memberships": "memberships.csv",
    "raw_rests": "rests.csv",
    "raw_shifts": "shifts.csv",
    "raw_user_contracts": "user_contracts.csv",
}

con = duckdb.connect(DB_PATH)

for table_name, file_name in FILES.items():
    csv_path = RAW_DIR / file_name
    print(f"Loading {csv_path} -> {table_name}")

    con.execute(f"""
        create or replace table {table_name} as
        select *
        from read_csv_auto('{csv_path.as_posix()}')
    """)

print("\nLoaded tables:")
print(con.sql("show tables").df())