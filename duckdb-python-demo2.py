import duckdb
import pandas as pd
import plotly.express as px

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


def duckdb_plt_bar():
    conn = duckdb.connect("somedb.ddb")
    conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM read_csv_auto('mydata.csv')")
    # result = conn.execute("SELECT job, count(*) FROM people group by job;").fetchall()
    result_df = conn.execute("SELECT job, count(*) FROM people group by job;").fetch_df()
    result_df.columns = ['job', 'count']

    # result_df = conn.execute("""
    #     SELECT job AS job, COUNT(*) AS count 
    #     FROM people 
    #     GROUP BY job
    # """).fetchdf()
    print(result_df)

    conn.close()  

    # Create an interactive bar chart
    fig = px.bar(
        result_df,
        x='job',
        y='count',
        title='Number of People by Job',
        labels={'job': 'Job', 'count': 'Number of People'},
        color='job',  # Optional: different colors for each job
    )

    # Customize layout
    fig.update_layout(
        xaxis_title="Job",
        yaxis_title="Number of People",
        xaxis={'tickangle': 45},
        showlegend=False  # Hide legend if using color per job
    )

    # Show the plot
    fig.show()
    # Plotly: Use fig.write_html('job_counts.html') for an HTML file or fig.write_image('job_counts.png') (requires pip install kaleido).
    # Colors: Customize colors in Matplotlib with color=['red', 'blue', ...] (list matching job count) or in Plotly via color_discrete_sequence.

def duckdb_plt_pie():
    # Connect to DuckDB
    conn = duckdb.connect()

    # Create table and execute query with age grouping
    conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM read_csv_auto('mydata.csv')")
    result = conn.execute("""
        SELECT 
            job AS job, 
            CASE 
                WHEN age < 30 THEN 'Under 30'
                WHEN age < 50 THEN '30-49'
                ELSE '50+' 
            END AS age_group,
            COUNT(*) AS count 
        FROM people 
        GROUP BY job, age_group
    """).fetchdf()

    # Close the connection
    conn.close()

    # Create a pie chart for each job
    for job in result['job'].unique():
        job_data = result[result['job'] == job]
        fig = px.pie(
            job_data,
            values='count',
            names='age_group',
            title=f'Age Distribution for {job}',
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        fig.update_traces(textinfo='percent+label', textposition='inside')
        fig.update_layout(showlegend=True, legend_title_text='Age Group')
        fig.show()

        
def duckdb_plt_pie2():
    # Connect to DuckDB
    conn = duckdb.connect()

    # Create table and execute query with age grouping
    conn.execute("CREATE TABLE IF NOT EXISTS people AS SELECT * FROM read_csv_auto('mydata.csv')")
    result = conn.execute("""
        SELECT job AS job, COUNT(*) AS count 
        FROM people 
        GROUP BY job
    """).fetchdf()

    # Close the connection
    conn.close()

    # Create a pie chart
    fig = px.pie(
        result,
        values='count',
        names='job',
        title='Distribution of People by Job',
        color_discrete_sequence=px.colors.qualitative.Plotly  # Optional: custom color scheme
    )

    # Customize layout
    fig.update_traces(
        textinfo='percent+label',  # Show percentage and job name on the pie
        textposition='inside'      # Place text inside the pie slices
    )
    fig.update_layout(
        showlegend=True,           # Show legend
        legend_title_text='Job'    # Legend title
    )

    # Show the plot
    fig.show()


if __name__ == '__main__':
    # sqlite3_demo()
    # duckdb_demo()
    # duckdb_sql()
    # duckdb_df()
    # duckdb_demo2()
    # duckdb_exec()
    # duckdb_plt_bar()
    # duckdb_plt_pie()
    duckdb_plt_pie2()