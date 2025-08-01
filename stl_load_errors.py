import configparser
import psycopg2
import pandas as pd

config = configparser.ConfigParser()
config.read('dwh.cfg')

query = """
    SELECT *
    FROM stl_load_errors
    ORDER BY starttime DESC
    LIMIT 10;
"""

query2 = """
    SELECT ordinal_position, column_name
    FROM information_schema.columns
    WHERE table_name = 'staging_events'
    ORDER BY ordinal_position;
"""

try:
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['DWH'].values()))
    cur = conn.cursor()
    print("✅ Connected to Redshift.")

    cur.execute(query)
    col_names = [desc[0] for desc in cur.description]
    rows = cur.fetchall()

    # Display results using pandas for readability
    df = pd.DataFrame(rows, columns=col_names)
    print(df.to_string(index=False))

    cur.close()
    conn.close()

except Exception as e:
    print("❌ Error while connecting or querying Redshift:")
    print(e)