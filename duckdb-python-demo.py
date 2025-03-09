import duckdb


def print_all_rows(con):
    # Query the table and fetch results
    result = con.execute("SELECT * FROM reject_codes").fetchall()

    # Print the results
    for row in result:
        print(row)


# Connect to an in-memory DuckDB instance
con = duckdb.connect()
# Read the CSV file into a DuckDB table
con.execute(
    "CREATE TABLE reject_codes AS SELECT * FROM read_csv_auto('reject-code.csv')"
)

print_all_rows(con)


# Close the connection (optional for in-memory DB)
con.close()
