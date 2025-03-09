import duckdb

# Define constants for headers
HEADER_CAPTURE_DATE = "capture date"
HEADER_RESPONSE_CODE = "response code"
HEADER_COUNT = "count"

# Define column widths
DATE_WIDTH = 15
CODE_WIDTH = 15
COUNT_WIDTH = 15
PERCENT_WIDTH = 15

# Output file name
OUTPUT_FILE = "reject_code_summary.txt"

# Connect to DuckDB
con = duckdb.connect()

# Execute the query
result = con.execute(f"""
    SELECT 
        "{HEADER_CAPTURE_DATE}",
        "{HEADER_RESPONSE_CODE}",
        SUM("{HEADER_COUNT}") AS total_rejections,
        100.0 * SUM("{HEADER_COUNT}") / SUM(SUM("{HEADER_COUNT}")) OVER (PARTITION BY "{HEADER_CAPTURE_DATE}") AS percentage
    FROM read_csv_auto('reject-code.csv')
    GROUP BY "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}"
    ORDER BY "{HEADER_CAPTURE_DATE}" ASC, SUM("{HEADER_COUNT}") DESC
""").fetchall()

# Open the file for writing
with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    # Write the header with alignment (no colors)
    header = (
        f"{HEADER_CAPTURE_DATE:<{DATE_WIDTH}} | "
        f"{HEADER_RESPONSE_CODE:<{CODE_WIDTH}} | "
        f"{'Total Rejections':<{COUNT_WIDTH}} | "
        f"{'Percentage':<{PERCENT_WIDTH}}"
    )
    f.write(header + "\n")
    f.write("-" * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + PERCENT_WIDTH + 6) + "\n")

    # Track the previous date and write rows with split lines
    previous_date = None
    for row in result:
        current_date = row[0]
        
        if previous_date is not None and current_date != previous_date:
            f.write("=" * (DATE_WIDTH + CODE_WIDTH + COUNT_WIDTH + PERCENT_WIDTH + 6) + "\n")
        
        date_str = str(current_date)
        code_str = str(row[1])
        count_str = str(row[2])
        percent_str = f"{row[3]:.2f}%"
        row_text = (
            f"{date_str:<{DATE_WIDTH}} | "
            f"{code_str:<{CODE_WIDTH}} | "
            f"{count_str:<{COUNT_WIDTH}} | "
            f"{percent_str:<{PERCENT_WIDTH}}"
        )
        f.write(row_text + "\n")
        
        previous_date = current_date

# Close the connection
con.close()

print(f"Results have been written to {OUTPUT_FILE}")