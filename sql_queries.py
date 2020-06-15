import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
ARN = config.get('IAM_ROLE','ROLE_ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

# STAGING TABLES
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender CHAR,
                                itemInSession SMALLINT,
                                lastName VARCHAR,
                                length DOUBLE PRECISION,
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
                                userId INTEGER)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs INTEGER,
                                artist_id VARCHAR,
                                artist_latitude FLOAT,
                                artist_longitude FLOAT,
                                artist_location VARCHAR,
                                artist_name VARCHAR,
                                song_id VARCHAR,
                                title VARCHAR,
                                duration FLOAT,
                                year INTEGER)""")

# FINAL TABLES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL REFERENCES time (start_time),
                            user_id INTEGER NOT NULL REFERENCES users (user_id),
                            level VARCHAR NOT NULL,
                            song_id VARCHAR NOT NULL REFERENCES song (song_id),
                            artist_id VARCHAR NOT NULL REFERENCES artist (artist_id),
                            session_id INTEGER NOT NULL,
                            location VARCHAR,
                            user_agent VARCHAR NOT NULL)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        first_name VARCHAR NOT NULL,
                        last_name VARCHAR NOT NULL,
                        gender CHAR NOT NULL,
                        level VARCHAR NOT NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (
                        song_id VARCHAR PRIMARY KEY,
                        title VARCHAR NOT NULL,
                        artist_id VARCHAR NOT NULL,
                        year INTEGER NOT NULL,
                        duration FLOAT NOT NULL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                          artist_id VARCHAR PRIMARY KEY,
                          name VARCHAR NOT NULL,
                          location VARCHAR,
                          latitude FLOAT,
                          longitude FLOAT)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP PRIMARY KEY,
                        hour SMALLINT NOT NULL,
                        day SMALLINT NOT NULL,
                        week SMALLINT NOT NULL,
                        month SMALLINT NOT NULL,
                        year SMALLINT NOT NULL,
                        weekday SMALLINT NOT NULL)""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                          credentials 'aws_iam_role={}'
                          format as JSON {}
                          timeformat as 'epochmillisecs'
                          region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""copy staging_songs from {}
                         credentials 'aws_iam_role={}'
                         format as JSON 'auto'
                         region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id,
                                                    session_id, location, user_agent)
                            SELECT
                            e.ts as start_time,
                            e.userId as user_id,
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId as session_id,
                            e.location,
                            e.userAgent as user_agent
                            FROM staging_events e join staging_songs s ON
                            (e.artist = s.artist_name AND
                            e.song = s.title AND
                            e.length = s.duration)
                            WHERE e.page = 'NextSong'""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                        (SELECT
                        userId as user_id,
                        firstName as first_name,
                        lastName as last_name,
                        gender,
                        level
                        FROM staging_events
                        WHERE page = 'NextSong')""")

song_table_insert = ("""INSERT INTO song (song_id, title, artist_id, year, duration)
                        SELECT
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration
                        FROM staging_songs""")

artist_table_insert = ("""INSERT INTO artist (artist_id, name, location, latitude, longitude)
                          SELECT
                          artist_id,
                          artist_name as name,
                          artist_location as location,
                          artist_latitude as latitude,
                          artist_longitude as longitude
                          FROM staging_songs""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT
                        ts as start_time,
                        EXTRACT(hour FROM ts) as hour,
                        EXTRACT(day FROM ts) as day,
                        EXTRACT(week FROM ts) as week,
                        EXTRACT(month FROM ts) as month,
                        EXTRACT(year FROM ts) as year,
                        EXTRACT(weekday FROM ts) as weekday
                        FROM staging_events
                        WHERE page = 'NextSong'""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_songs_copy, staging_events_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]