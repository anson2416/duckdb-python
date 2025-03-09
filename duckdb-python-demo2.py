import duckdb
import pandas as pd

def sqlite3_demo():
    import sqlite3

    conn = sqlite3.connect("somedb.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS person (id INT, name TEXT);")
    cur.execute("INSERT INTO person values(1,'Mike');")


    conn.commit()
    cur.close()
    conn.close()


def duckdb_demo():
    conn = duckdb.connect("somedb.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS person (id INT, name TEXT);")
    cur.execute("INSERT INTO person values(1,'Mike');")


    conn.commit()
    cur.close()
    conn.close()


def duckdb_sql():
    # print(duckdb.read_csv("mydata.csv"))
    print(duckdb.sql('SELECT * FROM "mydata.csv" WHERE AGE > 40;'))

def duckdb_df():
    df = pd.read_csv("mydata.csv")
    print(duckdb.sql("SELECT * FROM df WHERE age > 40;"))
    result = duckdb.sql("SELECT * FROM df WHERE age > 40;")
    print(result.fetchall())
    print(result.df())



def duckdb_demo2():
    conn = duckdb.connect("somedb.ddb")
    conn.sql("CREATE TABLE IF NOT EXISTS people as SELECT * FROM 'mydata.csv';")

    # then can use sql to query the data
    print(conn.execute("SELECT * FROM people;").fetchall())
    
    
    conn.commit()
    conn.close()

    
def duckdb_exec():
    conn = duckdb.connect("somedb.ddb")
    conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM read_csv_auto('mydata.csv')")
    print(conn.execute("SELECT * FROM people;").fetchall())
    print(conn.execute("SELECT * FROM people WHERE age>40;").fetchall())

    conn.close()  


def duckdb_plt():
    conn = duckdb.connect("somedb.ddb")
    conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM read_csv_auto('mydata.csv')")
    print(conn.execute("SELECT job, count(*) FROM people group by job;").fetchall())

    conn.close()  


if __name__ == '__main__':
    # sqlite3_demo()
    # duckdb_demo()
    # duckdb_sql()
    # duckdb_df()
    # duckdb_demo2()
    # duckdb_exec()
    duckdb_plt()