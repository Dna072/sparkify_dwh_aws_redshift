import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_times"

# CREATE TABLES

staging_events_table_create = (
    """
        CREATE TABLE IF NOT EXISTS staging_events (
            artist text,
            auth text,
            first_name varchar(20),
            gender varchar(3),
            item_in_session integer,
            last_name varchar(20),
            length double precision,
            level varchar(10),
            location text,
            method varchar(5),
            page varchar(50),
            registration numeric(15),
            session_id integer,
            song text,
            status integer,
            ts numeric(20),
            user_agent text,
            user_id integer
        )
        DISTSTYLE EVEN; -- staging tables for temporary processing only
    """
)

staging_songs_table_create = (
    """
        CREATE TABLE IF NOT EXISTS staging_songs (
            num_songs integer,
            artist_id varchar(20),
            artist_latitude double precision,
            artist_longitude double precision,
            artist_location text,
            artist_name varchar(255),
            song_id varchar(50),
            title varchar(255),
            duration numeric(10,5),
            year integer
        )
        DISTSTYLE EVEN; -- staging tables for temporary processing only
    """
)

songplay_table_create = (
    """
        CREATE TABLE IF NOT EXISTS fact_songplays (
            songplay_id INTEGER IDENTITY(0, 1) PRIMARY KEY,
            start_time timestamp NOT NULL SORTKEY, --sort on this column
            user_id integer REFERENCES dim_users(user_id) NOT NULL, --references not enforced in Redshift but to maintain logical data integrity
            level varchar(10) NOT NULL,
            song_id varchar(50) REFERENCES dim_songs(song_id) NOT NULL,
            artist_id varchar(50) REFERENCES dim_artists(artist_id) NOT NULL,
            session_id integer NOT NULL,
            location varchar(255) NOT NULL,
            user_agent varchar(255) NOT NULL
        )
        DISTSTYLE AUTO;  -- To avoid manual skew, let's Redshift optimize and then based on our usage we can decide to use KEY diststyle on the right column.
    """
)

user_table_create = (
    """
        CREATE TABLE IF NOT EXISTS dim_users (
            user_id integer PRIMARY KEY SORTKEY,
            first_name varchar(20) NOT NULL, 
            last_name varchar(20) NOT NULL,
            gender varchar(1) NOT NULL,
            level varchar(10) NOT NULL
        )
        DISTSTYLE ALL;   -- very small table so full replication on all slices will improve query performance
    """
)

song_table_create = (
    """
        CREATE TABLE dim_songs (
            song_id varchar(50) PRIMARY KEY, 
            title varchar(255) NOT NULL SORTKEY, 
            artist_id varchar(50) NOT NULL DISTKEY, -- artist_id gives a relatively less skewed distribution 
            year integer, 
            duration numeric(10,5) NOT NULL
        )
    """
)

artist_table_create = (
    """
    CREATE TABLE dim_artists (
        artist_id varchar(50) PRIMARY KEY, 
        artist_name varchar(255) NOT NULL SORTKEY, --sort on this column
        artist_location varchar(255), 
        latitude double precision, 
        longitude double precision
    )
    DISTSTYLE AUTO; -- auto selected as there's no clear distkey to avoid skewing on slices. Let Redshift decide for now.
    """
)

time_table_create = (
    """
        CREATE TABLE dim_times (
            time_id INTEGER IDENTITY(0,1) PRIMARY KEY, --primary key not enforced in redshift but for clarity
            start_time timestamp NOT NULL SORTKEY, --sort on start_time
            hour integer NOT NULL, 
            day integer NOT NULL,
            week integer NOT NULL, 
            month integer NOT NULL, 
            year integer NOT NULL, 
            weekday integer NOT NULL
        )
        DISTSTYLE AUTO; --To avoid manual skew, let's Redshift optimize
    """
)

# STAGING TABLES
staging_events_copy = (
    """
    COPY staging_events FROM 's3://udacity-dend/log_data'
    IAM_ROLE {}
    REGION 'us-west-2'
    FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
    """
).format(DWH_ROLE_ARN)

staging_songs_copy = (
    """
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    IAM_ROLE {}
    REGION 'us-west-2'
    FORMAT AS JSON 'auto'
    """
).format(DWH_ROLE_ARN)

# FINAL TABLES

songplays_table_insert = (
    """
        INSERT INTO fact_songplays (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )
        SELECT
            TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.user_id,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.session_id,
            se.location,
            se.user_agent
        FROM staging_events se
        JOIN staging_songs ss
        ON se.song = ss.title
        AND se.artist = ss.artist_name
        WHERE se.page = 'NextSong'
        AND se.user_id IS NOT NULL;
    """
)
# The ABS(ss.length - ss.duration ) < 2.0 is used to handle minor inconsistencies in duration between
# logs and metadata

user_table_insert = (
    """
        INSERT INTO dim_users (
            user_id, 
            first_name, 
            last_name, 
            gender, 
            level
        )
        SELECT
            distinct(user_id),
            first_name,
            last_name,
            gender,
            level
        FROM staging_events 
        WHERE user_id IS NOT NULL
        AND page = 'NextSong'           --ensure only users who played songs are stored
        ORDER BY user_id, ts DESC;      --pick the latest user record per user_id
    """
)

song_table_insert = (
    """
        INSERT INTO dim_songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )
        SELECT 
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
        WHERE song_id IS NOT NULL;
    """
)

artist_table_insert = (
    """
        INSERT INTO dim_artists (
            artist_id,
            artist_name,
            artist_location,
            latitude,
            longitude
        )
        SELECT 
            distinct(artist_id),
            artist_name,
            artist_location,
            artist_latitude as latitude,
            artist_longitude as longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """
)

time_table_insert = (
    """
        INSERT INTO dim_times (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT 
            distinct (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS start_time,
            EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
            EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
            EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
            EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
            EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
            EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
        FROM staging_events
        WHERE ts IS NOT NULL;
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        user_table_create, song_table_create, artist_table_create,
                        time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplays_table_insert, user_table_insert, song_table_insert,
                        artist_table_insert, time_table_insert]
