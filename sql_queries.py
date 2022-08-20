import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
users_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (artist varchar,
                                          auth varchar,
                                          firstName varchar,
                                          gender varchar,
                                          itemInSession int,
                                          lastName varchar,
                                          length float,
                                          level varchar,
                                          location varchar,
                                          method varchar,
                                          page varchar,
                                          registration float,
                                          sessionId int,
                                          song varchar ,
                                          status int,
                                          ts bigint,
                                          userAgent varchar,
                                          userId int);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (   num_songs int,
                                             artist_id varchar,
                                             artist_latitude float,
                                             artist_longitude float,
                                             artist_location varchar,
                                             artist_name varchar,
                                             song_id varchar,
                                             title varchar,
                                             duration float,
                                             year int)
                                            """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, \
                                                         first_name varchar, \
                                                         last_name varchar, \
                                                         gender varchar, \
                                                         level varchar);
                                                         """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, \
                                                        title varchar NOT NULL, \
                                                        artist_id varchar NOT NULL, \
                                                        year int , \
                                                        duration float);
                                                        """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists( artist_id varchar PRIMARY KEY, \
                                                             name varchar NOT NULL, \
                                                             location varchar, \
                                                             latitude varchar, \
                                                             longitude varchar);
                                                             """)
time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP PRIMARY KEY, \
                                                        hour int, \
                                                        day int, \
                                                        week int, \
                                                        month int, \
                                                        year int, \
                                                        weekday int);
                                                        """)

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id varchar PRIMARY KEY , \
                                                                 start_time TIMESTAMP NOT NULL , \
                                                                 user_id int NOT NULL  , \
                                                                 level varchar NOT NULL, \
                                                                 song_id varchar NOT NULL  , \
                                                                 artist_id varchar NOT NULL , \
                                                                 session_id int NOT NULL , \
                                                                 location varchar NOT NULL, \
                                                                 user_agent varchar NOT NULL);
                                                                 """)




staging_events_copy = """
COPY staging_events
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS JSON '{}';
""".format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])
staging_songs_copy = """
COPY staging_songs
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
JSON 'auto';
""".format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])



# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, 
        staging_events.userId, 
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id,
        staging_events.sessionId,
        staging_events.location,
        staging_events.userAgent
    FROM staging_events
    JOIN staging_songs on (staging_events.artist =  staging_songs.artist_name)
    AND (staging_events.song = staging_songs.title)
    JOIN songplays on (songplays.artist_id = staging_songs.artist_id)
    WHERE staging_events.page = 'NextSong'
    and staging_events.location is not Null
    and staging_events.useragent is not Null
    and staging_songs.artist_id is not Null
    and staging_events.userId is not Null
    and staging_events.level is not Null
    and staging_songs.song_id is not Null
    and staging_events.sessionid is not Null
    and songplays.songplay_id is not null
""")







 
users_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  
    SELECT DISTINCT 
        userId,
        firstName,
        lastName,
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND userId NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude 
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT 
        start_time, 
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time FROM staging_events  
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, users_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, users_table_insert, song_table_insert, artist_table_insert, time_table_insert]
