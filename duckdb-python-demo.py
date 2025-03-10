import duckdb
# ANSI color codes
BLUE = "\033[94m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
RESET = "\033[0m"  # Resets color to default


HEADER_CAPTURE_DATE = "capture date"
HEADER_RESPONSE_CODE= "response code"
HEADER_COUNT = "count"

# Define column widths (adjust these based on your data)
DATE_WIDTH = 15  # Width for "capture date"
CODE_WIDTH = 15  # Width for "response code"
COUNT_WIDTH = 15  # Width for "Total Rejections"
PERCENT_WIDTH = 15

# DATE_WIDTH = max(len(HEADER_CAPTURE_DATE), max(len(str(row[0])) for row in result))
# CODE_WIDTH = max(len(HEADER_RESPONSE_CODE), max(len(str(row[1])) for row in result))
# COUNT_WIDTH = max(len("Total Rejections"), max(len(str(row[2])) for row in result))



# Print the header with colors and alignment
header = (
    f"{BLUE}{HEADER_CAPTURE_DATE:<{DATE_WIDTH}}{RESET} | "
    f"{GREEN}{HEADER_RESPONSE_CODE:<{CODE_WIDTH}}{RESET} | "
    f"{YELLOW}{'Total Rejections':<{COUNT_WIDTH}}{RESET}"
)

def print_all_rows(con):
    # Query the table and fetch results
    result = con.execute("SELECT * FROM table_reject_codes").fetchall()

    # Print the results
    for row in result:
        print(row)

def get_all_rows(con, sql):
    # Query the table and fetch results
    # print(f"sql={sql}")
    result = con.execute(sql).fetchall()

    # Print the results
    # for row in result:
        # print(row)

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
sql_results.write_csv("result-group-by-capture-date-rejected.csv")  


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
# print(f"{BLUE}{HEADER_CAPTURE_DATE}{RESET} | {GREEN}{HEADER_RESPONSE_CODE}{RESET} | {YELLOW}Total Rejections{RESET}")
# print("---------------------------------------------")
print(header)
print("-" * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + 4))  # Adjust separator length
for row in sql_results:
    # print(f"{row[0]} | {row[1]} | {row[2]}")
    # print(f"{BLUE}{row[0]}{RESET} | {GREEN}{row[1]}{RESET} | {YELLOW}{row[2]}{RESET}")
    date_str = str(row[0])  # Convert to string to handle different types
    code_str = str(row[1])
    count_str = str(row[2])
    print(
        f"{BLUE}{date_str:<{DATE_WIDTH}}{RESET} | "
        f"{GREEN}{code_str:<{CODE_WIDTH}}{RESET} | "
        f"{YELLOW}{count_str:<{COUNT_WIDTH}}{RESET}"
    )


sql = f"""
SELECT "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}", SUM("{HEADER_COUNT}") as total_rejections
    FROM table_reject_codes
    WHERE "{HEADER_RESPONSE_CODE}" <> '0000'
    GROUP BY "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}"
    ORDER BY "{HEADER_CAPTURE_DATE}" asc, sum("{HEADER_COUNT}") desc
"""
sql_results = get_all_rows(con, sql)


result = con.execute(f"""
    SELECT 
        "{HEADER_CAPTURE_DATE}",
        "{HEADER_RESPONSE_CODE}",
        SUM("count") AS total_rejections,
        100.0 * SUM("count") / SUM(SUM("count")) OVER (PARTITION BY "{HEADER_CAPTURE_DATE}") AS percentage
    FROM read_csv_auto('reject-code.csv')
    GROUP BY "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}"
    ORDER BY "{HEADER_CAPTURE_DATE}" ASC, SUM("count") DESC
""").fetchall()
# Print each row with aligned columns and colors
print(header)
print("-" * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + PERCENT_WIDTH + 6))  # Adjust separator
sql_results = get_all_rows(con, sql)

# Track the previous date and print rows with split lines
previous_date = None
for row in result:
    current_date = row[0]
    # Add a split line if the date changes
    if previous_date is not None and current_date != previous_date:
        # print("=" * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + PERCENT_WIDTH + 6))  # Split line
        print(f"{BLUE}{'=' * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + PERCENT_WIDTH + 6)}{RESET}")  # Blue split line
    
    # Format and print the current row
    date_str = str(row[0])
    code_str = str(row[1])
    count_str = str(row[2])
    percent_str = f"{row[3]:.2f}%"  # Format percentage with 2 decimal places
    print(
        f"{BLUE}{date_str:<{DATE_WIDTH}}{RESET} | "
        f"{GREEN}{code_str:<{CODE_WIDTH}}{RESET} | "
        f"{YELLOW}{count_str:<{COUNT_WIDTH}}{RESET} | "
        f"{MAGENTA}{percent_str:<{PERCENT_WIDTH}}{RESET}"
    )
    # Update the previous date
    previous_date = current_date


# Close the connection (optional for in-memory DB)
con.close()
