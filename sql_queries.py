import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
REGION = config.get("LOCATION", "REGION")


# DROP TABLES
staging_events_table_drop = " DROP TABLE  IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE  IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE  IF EXISTS songplay_table"
user_table_drop = "DROP TABLE  IF EXISTS user_table"
song_table_drop = "DROP TABLE  IF EXISTS song_table"
artist_table_drop = "DROP TABLE  IF EXISTS artist_table"
time_table_drop = "DROP TABLE  IF EXISTS  time_table"

# CREATE TABLES
staging_events_table_create = (""" 
CREATE TABLE IF NOT EXISTS staging_events_table
(
 artist varchar,
 auth varchar,
 firstName varchar,
 gender varchar,
 ItemInSession int,
 lastName varchar,
 length numeric,
 level varchar,
 location varchar,
 method varchar,
 page varchar,
 registration varchar,
 sessionId int,
 song varchar,
 status int,
 ts BIGINT,
 userAgent varchar,
 userId int
);
""")



staging_songs_table_create = (""" 
CREATE TABLE IF NOT EXISTS staging_songs_table
(
id bigint IDENTITY ,
num_songs int  ,
artist_id varchar ,
artist_latitude float  ,
artist_longitude float,
artist_location varchar(max),
artist_name varchar(max),
song_id varchar,
title varchar(max),
duration float,
year FLOAT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
(
songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY, 
start_time timestamp,
user_id varchar ,
level varchar,
song_id varchar  ,
artist_id varchar(max)  ,
session_id varchar NOT NULL,
location varchar(max),
user_agent varchar(max)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table
(
user_id varchar PRIMARY KEY,
first_name varchar,
last_name varchar,
gender char,
level varchar NOT NULL
 );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
(
song_id varchar PRIMARY KEY ,
title varchar(max),
artist_id varchar,
year int,
duration numeric
);
""")

artist_table_create = (""" 
CREATE TABLE IF NOT EXISTS artist_table
(
artist_id varchar PRIMARY KEY,
name varchar(max),
location varchar(max),
latitude numeric,
longitude numeric
);
""")

time_table_create = (""" 
CREATE TABLE IF NOT EXISTS time_table(
start_time TIMESTAMP PRIMARY KEY
, hour int NOT NULL
, day int NOT NULL
, week int NOT NULL
, month int NOT NULL
, year int NOT NULL
, weekday varchar  NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""  
COPY staging_events_table FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION {}
FORMAT AS JSON {}
dateformat 'auto'
TIMEFORMAT 'epochmillisecs'                         
""").format(LOG_DATA, IAM_ROLE, REGION, LOG_JSONPATH)

staging_songs_copy = ("""   
COPY staging_songs_table FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION {}
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(SONG_DATA, IAM_ROLE, REGION)

# FINAL TABLES

songplay_table_insert = (""" 
INSERT INTO songplay_table( start_time, user_id, level, song_id,artist_id, session_id, location, user_agent)
SELECT DISTINCT
 TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time,
 se.userId as user_id,
 se.level as level ,
 ss.song_id as song_id ,
 ss.artist_id as artist_id ,
 se.sessionId as session_id ,
 se.location as location ,
 se.userAgent as user_agent
 FROM staging_events_table se
 JOIN staging_songs_table ss
 ON se.song = ss.title AND se.artist = ss.artist_name
 WHERE se.page = 'NextSong'
""")

user_table_insert = (""" 
INSERT INTO user_table
(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
se.userId, 
se.firstName , 
se.lastName , 
se.gender , 
se.level
FROM staging_events_table se
WHERE se.userId IS NOT NULL
""")

song_table_insert = (""" 
INSERT INTO song_table
(song_id, title, artist_id, year, duration)
SELECT DISTINCT
ss.song_id ,
ss.title , 
ss.artist_id , 
ss.year, 
ss.duration
FROM staging_songs_table ss
Where ss.song_id IS NOT NULL
""")

artist_table_insert = (""" 
INSERT INTO artist_table
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT
ss.artist_id , 
ss.artist_name , 
ss.artist_location,
ss.artist_latitude , 
ss.artist_longitude
FROM staging_songs_table ss
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
insert into time_table (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (se.ts/1000) * INTERVAL '1 second' AS start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events_table se
WHERE se.ts IS NOT NULL AND se.page = 'NextSong';
""")



# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
                      user_table_drop,song_table_drop, artist_table_drop, time_table_drop]

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create,
                        artist_table_create, time_table_create]

copy_table_queries = [staging_events_copy, staging_songs_copy]
# copy_table_queries1 = [staging_events_copy]
# copy_table_queries2 = [staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
