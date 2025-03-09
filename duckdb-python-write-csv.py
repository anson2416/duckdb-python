# Persistent Storage
import duckdb

# Define constants for headers
HEADER_CAPTURE_DATE = "capture date"
HEADER_RESPONSE_CODE = "response code"

# Output CSV file name
OUTPUT_CSV = "reject_code_summary.csv"

# Connect to DuckDB
con = duckdb.connect()

# Execute the query and write to CSV using COPY
# con.execute(f"""
#     COPY (
#         SELECT 
#             "{HEADER_CAPTURE_DATE}",
#             "{HEADER_RESPONSE_CODE}",
#             SUM("count") AS total_rejections,
#             100.0 * SUM("count") / SUM(SUM("count")) OVER (PARTITION BY "{HEADER_CAPTURE_DATE}") AS percentage
#         FROM read_csv_auto('reject-code.csv')
#         GROUP BY "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}"
#         ORDER BY "{HEADER_CAPTURE_DATE}" ASC, SUM("count") DESC
#     ) TO '{OUTPUT_CSV}' (HEADER, DELIMITER ',');
# """)

# Execute the query and write to CSV with formatted percentages
# Add formatting in the query for percentage
con.execute(f"""
    COPY (
        SELECT 
            "{HEADER_CAPTURE_DATE}",
            "{HEADER_RESPONSE_CODE}",
            SUM("count") AS total_rejections,
            CAST(ROUND(100.0 * SUM("count") / SUM(SUM("count")) OVER (PARTITION BY "{HEADER_CAPTURE_DATE}"), 2) AS STRING) || '%' AS percentage
        FROM read_csv_auto('reject-code.csv')
        GROUP BY "{HEADER_CAPTURE_DATE}", "{HEADER_RESPONSE_CODE}"
        ORDER BY "{HEADER_CAPTURE_DATE}" ASC, SUM("count") DESC
    ) TO '{OUTPUT_CSV}' (HEADER, DELIMITER ',');
""")


# Close the connection
con.close()

print(f"Results have been written to {OUTPUT_CSV}")