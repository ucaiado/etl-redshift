#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
SQL quries to use in ETL


@author: udacity, ucaiado

Created on 04/25/2020
"""

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('confs/dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist text,
        auth text,
        first_name text,
        gender text,
        item_in_session int,
        last_name text,
        length float,
        level text,
        location text,
        method text,
        page text,
        registration text,
        session_id text,
        song text,
        status text,
        ts bigint,
        user_agent text,
        user_id int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id text,
        artist_latitude float,
        artist_longitude float,
        artist_location text,
        artist_name text,
        song_id text,
        title text,
        duration float,
        year int
    );
""")

# NOTE: https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int IDENTITY(0,1),
        start_time bigint,
        user_id int NOT NULL,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id varchar NOT NULL,
        location varchar,
        user_agent text,
        PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id int NOT NULL,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar,
        PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar NOT NULL,
        title varchar,
        artist_id varchar,
        year int,
        duration float,
        PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar NOT NULL,
        name varchar,
        location varchar,
        latitude float,
        longitude float,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time bigint NOT NULL,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int,
        PRIMARY KEY (start_time)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
        credentials 'aws_iam_role={}'
        json {}
        region 'us-west-2';
""").format(
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH']
)


# NOTE: https://knowledge.udacity.com/questions/144884
staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
        credentials 'aws_iam_role={}'
        json 'auto' truncatecolumns
        maxerror as 250
        region 'us-west-2';
""").format(
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN']
)


# FINAL TABLES


songplay_table_insert = ("""
    INSERT INTO songplays (
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
        events.ts,
        events.user_id,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.session_id,
        events.location,
        events.user_agent
    FROM staging_events AS events
    INNER JOIN staging_songs AS songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
    WHERE events.page = 'NextSong'
    AND events.session_id NOT IN (
        SELECT DISTINCT events.session_id FROM songplays
        WHERE start_time = events.ts
        AND user_id = events.user_id
        AND session_id = events.session_id
    );
""")



# NOTE: Based on project 1, we should filter only NextSong pages
# NOTE: The second clause allows to run code multiple times without
#    duplicate data
user_table_insert = ("""
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        user_id,
        first_name,
        last_name,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
    AND user_id NOT IN (
        SELECT DISTINCT user_id FROM users
    );
""")

song_table_insert = ("""
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id NOT IN (
        SELECT DISTINCT song_id FROM songs
    );
""")


artist_table_insert = ("""
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (
        SELECT DISTINCT artist_id FROM artists
    );
""")


# NOTE: time transformation - https://knowledge.udacity.com/questions/74200
time_table_insert = ("""
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        this_timestamp,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM (
        SELECT DISTINCT
            TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time,
            ts AS this_timestamp
        FROM staging_events
    )
    WHERE start_time NOT IN (
        SELECT DISTINCT start_time FROM time
    );
""")


# QUERY LISTS

create_table_queries = [
    staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create]

drop_table_queries = [
    staging_events_table_drop, staging_songs_table_drop, songplay_table_drop,
    user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    songplay_table_insert, user_table_insert, song_table_insert,
    artist_table_insert, time_table_insert]
