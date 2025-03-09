import duckdb
# ANSI color codes
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RESET = "\033[0m"  # Resets color to default

HEADER_CAPTURE_DATE = "capture date"
HEADER_RESPONSE_CODE= "response code"
HEADER_COUNT = "count"

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
# print("Capture Date | Response Code | Total Rejections")
print(f"{BLUE}{HEADER_CAPTURE_DATE}{RESET} | {GREEN}{HEADER_RESPONSE_CODE}{RESET} | {YELLOW}Total Rejections{RESET}")
print("---------------------------------------------")
for row in sql_results:
    # print(f"{row[0]} | {row[1]} | {row[2]}")
    print(f"{BLUE}{row[0]}{RESET} | {GREEN}{row[1]}{RESET} | {YELLOW}{row[2]}{RESET}")



# Close the connection (optional for in-memory DB)
con.close()
