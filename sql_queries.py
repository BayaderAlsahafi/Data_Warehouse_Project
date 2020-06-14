import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender char,
                                itemInSession smallint,
                                lastName varchar,
                                length double precision,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration float,
                                sessionId int,
                                song varchar,
                                status int,
                                ts int,
                                userAgent varchar,
                                userId int)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs int,
                                artist_id varchar,
                                artist_latitude varchar,
                                artist_longitude varchar,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration double precision,
                                year int)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (
                            songplay_id INT IDENTITY(0,1) PRIMARY KEY,
                            start_time INT NOT NULL,
                            user_id INT NOT NULL REFERENCES user (user_id),
                            level VARCHAR NOT NULL,
                            song_id INT NOT NULL REFERENCES song (song_id),
                            artist_id INT NOT NULL REFERENCES artist (artist_id),
                            session_id INT NOT NULL,
                            location VARCHAR NOT NULL,
                            user_agent VARCHAR NOT NULL)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS user (
                        user_id INT IDENTITY(0,1) PRIMARY KEY,
                        first_name VARCHAR NOT NULL,
                        last_name VARCHAR NOT NULL,
                        gender VARCHAR NOT NULL,
                        level VARCHAR NOT NULL)""")

song_table_create = ("""CREATE TABLE IF NOT EXSISTS song (
                        song_id INT IDENTITY(0,1) PRIMARY KEY,
                        title VARCHAR NOT NULL,
                        artist_id INT NOT NULL REFERENCES artist(artist_id),
                        year INT NOT NULL,
                        duration DOUBLE NOT NULL)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (
                          artist_id INT IDENTITY(0,1) PRIMARY KEY,
                          name VARCHAR NOT NULL,
                          location VARCHAR NOT NULL,
                          lattitude VARCHAR,
                          longitude VARCHAR)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time INT PRIMARY KEY,
                        hour SMALLINT NOT NULL,
                        day SMALLINT NOT NULL,
                        week SMALLINT NOT NULL,
                        month SMALLINT NOT NULL,
                        year SMALLINT NOT NULL,
                        weekday SMALLINT NOT NULL)""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
