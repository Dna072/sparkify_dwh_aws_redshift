import configparser
import psycopg2
import pandas as pd
import time

# Cluster configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Dictionary of example queries
example_queries = {
    "Top 10 Most Played Songs": """
        SELECT s.title, COUNT(*) AS play_count
        FROM fact_songplays sp
        JOIN dim_songs s ON sp.song_id = s.song_id
        GROUP BY s.title
        ORDER BY play_count DESC
        LIMIT 10;
    """,
    "Top 10 Most Active Users": """
        SELECT u.user_id, u.first_name, u.last_name, COUNT(*) AS play_count
        FROM fact_songplays sp
        JOIN dim_users u ON u.user_id = sp.user_id
        GROUP BY u.user_id, u.first_name, u.last_name
        ORDER BY play_count DESC
        LIMIT 10;
    """,
    "Most Popular Artists": """
        SELECT a.artist_name, COUNT(*) AS play_count
        FROM fact_songplays sp
        JOIN dim_artists a ON sp.artist_id = a.artist_id
        GROUP BY a.artist_name
        ORDER BY play_count DESC
        LIMIT 10; 
    """,
    "Top 10 locations by Song Plays": """
        SELECT location, COUNT(*) AS play_count
        FROM fact_songplays sp
        GROUP BY location
        ORDER BY play_count DESC
        LIMIT 10;
    """,
    "Song Plays by Weekday": """
        SELECT t.weekday, TRIM(TO_CHAR(t.start_time, 'Day')) AS weekday_name, COUNT(*) AS play_count
        FROM fact_songplays sp
        JOIN dim_times t ON sp.start_time = t.start_time
        GROUP BY t.weekday, weekday_name
        ORDER BY t.weekday;
    """
}


def run_queries():
    """
      Connects to the Redshift cluster and executes a series of predefined SQL queries.

      For each query:
      - Measures and prints the query execution time.
      - Fetches and displays the result as a formatted pandas DataFrame.

      Closes the database connection after all queries are executed.

      Raises:
          Prints an error message if the connection or query execution fails.
      """
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                .format(*config['DWH'].values()))
        cur = conn.cursor()
        print("✅ Connected to Redshift.")

        for title, query in example_queries.items():
            print(f"\n 📊 {title}")
            start_time = time.time()
            cur.execute(query)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            duration = time.time() - start_time

            df = pd.DataFrame(rows, columns=colnames)
            print(f"⏱️ Query time: {duration:.4f} seconds\n")
            print(df)

        cur.close()
        conn.close()

    except Exception as e:
        print("❌ Error:", e)


if __name__ == "__main__":
    run_queries()