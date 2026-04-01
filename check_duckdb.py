import duckdb

DB_PATH = "data/warehouse.duckdb"

con = duckdb.connect(DB_PATH)

print("Connected to:", DB_PATH)
print("\nTables:")
print(con.sql("show tables").df())