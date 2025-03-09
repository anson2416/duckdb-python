import duckdb


def print_all_rows(con):
    # Query the table and fetch results
    result = con.execute("SELECT * FROM table_reject_codes").fetchall()

    # Print the results
    for row in result:
        print(row)

def get_all_rows(con, sql):
    # Query the table and fetch results
    result = con.execute(sql).fetchall()

    # Print the results
    for row in result:
        print(row)

    return result
    

# Connect to an in-memory DuckDB instance
con = duckdb.connect()
# Read the CSV file into a DuckDB table
con.execute(
    "CREATE TABLE table_reject_codes AS SELECT * FROM read_csv_auto('reject-code.csv')"
)

# print_all_rows(con)

# sql = "SELECT * FROM table_reject_codes"
# sql_results = get_all_rows(con, sql)
HEADER_CAPTURE_DATE = "capture date"

sql = """
SELECT "capture date", "response code", SUM(count) as total_rejections
    FROM table_reject_codes
    WHERE "response code" <> '0000'
    GROUP BY "capture date", "response code"
    ORDER BY "capture date" asc, sum(count) desc
"""
sql_results = get_all_rows(con, sql)


sql = """
WITH RankedRejections AS (
    SELECT 
        "capture date",
        "response code",
        SUM(count) AS total_rejections,
        ROW_NUMBER() OVER (
            PARTITION BY "capture date" 
            ORDER BY SUM(count) DESC
        ) AS rank_num
    FROM read_csv_auto('reject-code.csv')
    WHERE "response code" <> '0000'
    GROUP BY "capture date", "response code"
)
SELECT 
    "capture date",
    "response code",
    total_rejections
FROM RankedRejections
WHERE rank_num <= 2
ORDER BY "capture date" ASC, total_rejections DESC;
"""
sql_results = get_all_rows(con, sql)
print("---------------------------------------------")
print("Top 2 reject code for each date")
print("Capture Date | Response Code | Total Rejections")
print("---------------------------------------------")
for row in sql_results:
    print(f"{row[0]} | {row[1]} | {row[2]}")



# Close the connection (optional for in-memory DB)
con.close()
