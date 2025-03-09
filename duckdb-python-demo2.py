import duckdb

# import sqlite3

conn = duckdb.connect("somedb.db")
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS person (id INT, name TEXT);")
cur.execute("INSERT INTO person values(1,'Mike');")


conn.commit()
cur.close()
conn.close()