import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
# IAM ROLE
ROLE_ARN = config['IAM_ROLE']['ARN']
# S3
LOG_DATA, LOG_JSON_PATH, SONG_DATA = config['S3'].values()

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# staging tables
staging_events_table_create= ("""
CREATE TABLE staging_events (
                        artist VARCHAR,
                        auth VARCHAR,
                        firstName VARCHAR,
                        gender VARCHAR,
                        itemInSession INTEGER,
                        lastName VARCHAR,
                        length FLOAT,
                        level VARCHAR,
                        location VARCHAR,
                        method VARCHAR,
                        page VARCHAR,
                        registration FLOAT,
                        sessionId INTEGER,
                        song VARCHAR,
                        status INTEGER,
                        ts TIMESTAMP,
                        userAgent VARCHAR,
                        userId INTEGER
                            )
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
                        num_songs INTEGER,
                        artist_id VARCHAR,
                        artist_latitude FLOAT,
                        artist_longitude FLOAT,
                        artist_location VARCHAR(65535),
                        artist_name VARCHAR(65535),
                        song_id VARCHAR,
                        title VARCHAR(65535),
                        duration FLOAT,
                        year INTEGER
                            )

""")

# fact table
songplay_table_create = ("""
CREATE TABLE songplays (
                    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                    start_time TIMESTAMP NOT NULL,
                    user_id INTEGER NOT NULL,
                    level VARCHAR(4) NOT NULL,
                    song_id VARCHAR(18) NOT NULL,
                    artist_id VARCHAR(18) NOT NULL,
                    session_id INTEGER NOT NULL,
                    location VARCHAR(65535),
                    user_agent VARCHAR(65535)
                )
DISTSTYLE AUTO;
""")

# dimension tables
user_table_create = ("""
CREATE TABLE users (
                    user_id INTEGER PRIMARY KEY,
                    first_name VARCHAR NOT NULL,
                    last_name VARCHAR NOT NULL,
                    gender VARCHAR,
                    level VARCHAR(4) NOT NULL
                )
DISTSTYLE AUTO;
""")

song_table_create = ("""
CREATE TABLE songs (
                    song_id VARCHAR(18) PRIMARY KEY,
                    title VARCHAR(65535) NOT NULL,
                    artist_id VARCHAR(18) NOT NULL,
                    year INTEGER NOT NULL,
                    duration FLOAT NOT NULL
                )
DISTSTYLE AUTO;
""")

artist_table_create = ("""
CREATE TABLE artists (
                artist_id VARCHAR(18) PRIMARY KEY,
                name VARCHAR(65535) NOT NULL,
                location VARCHAR(65535),
                latitude FLOAT,
                longitude FLOAT
                )
DISTSTYLE AUTO;
""")

time_table_create = ("""
CREATE TABLE time (
                start_time TIMESTAMP PRIMARY KEY,
                hour INTEGER NOT NULL,
                day INTEGER NOT NULL,
                week INTEGER NOT NULL,
                month INTEGER NOT NULL,
                year INTEGER NOT NULL,
                weekday VARCHAR(9) NOT NULL
                )
DISTSTYLE AUTO;
""")

# COPY data from S3 to staging tables

staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON {} REGION 'us-west-2'
TIMEFORMAT 'epochmillisecs';
""").format(LOG_DATA, ROLE_ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
JSON 'auto' REGION 'us-west-2';
""").format(SONG_DATA, ROLE_ARN)

# Insert data into fact and dimension tables

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id)
SELECT ts AS start_time,
       userId AS user_id,
       level,
       song_id,
       artist_id,
       sessionId AS session_id
FROM staging_events se JOIN staging_songs ss
ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE (start_time IS NOT NULL AND userId IS NOT NULL
AND level IS NOT NULL AND song_id IS NOT NULL
AND artist_id IS NOT NULL AND sessionId IS NOT NULL)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId AS user_id,
                firstName AS first_name,
                lastName AS last_name,
                gender,
                level
FROM staging_events
WHERE (userId IS NOT NULL AND firstName IS NOT NULL
AND lastName IS NOT NULL AND level IS NOT NULL)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE (song_id IS NOT NULL AND title IS NOT NULL
AND artist_id IS NOT NULL AND year IS NOT NULL
AND duration IS NOT NULL)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                artist_name AS name,
                artist_location AS location,
                artist_latitude AS latitude,
                artist_longitude AS longitude
FROM staging_songs
WHERE (artist_id IS NOT NULL AND artist_name IS NOT NULL)
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts AS start_time,
                EXTRACT(hour FROM start_time) AS hour,
                EXTRACT(day FROM start_time) AS day,
                EXTRACT(week FROM start_time) AS week,
                EXTRACT(month FROM start_time) AS month,
                EXTRACT(year FROM start_time) AS year,
                TO_CHAR(EXTRACT(weekday FROM start_time), 'Day') AS weekday
FROM staging_events
WHERE ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
