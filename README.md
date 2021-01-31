# Data warehouse with AWS

Using the song and event datasets, I have created a star schema optimized for queries on song play analysis.
This includes the following tables.The data is then stored on aws redshift.

# Fact table

The fact table consist of songplays. Values included in this table are as follow :
    songplays - records in event data associated with song plays i.e. records with page NextSong
    songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

# Dimension tables

The dimension tables include data about artist, song , time and user.

    Each table consist of the folowing : </p>
    -users - users in the app
        user_id, first_name, last_name, gender, level
    -songs - songs in music database
        song_id, title, artist_id, year, duration
    -artists - artists in music database
        artist_id, name, location, lattitude, longitude
    -time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday

# Project template

To get started with the project, go to the workspace on the next page, where you'll find the project template. You can work on yproject and submit your work through this workspace.

Alternatively, you can download the template files in the Resources tab in the classroom and work on this project on your local computer.

The project template includes five files:

- create_table.py is where I have created my fact and dimension tables for the star schema in Redshift.
- etl.py is where I have loaded data from S3 into staging tables on Redshift and then process that data into my analytics tables on Redshift.
- sql_queries.py is where I have defined my SQL statements, which will be imported into the two other files above.
- README.md is where I have provided my process and decisions for this ETL pipeline.
- dwh.cfg which contains aws config information

# How to run the project

Please run

    python create_table.py
    python etl.py

From your terminal . the rest would be done automatically.

# Enviroment

    Python 3.6.3
    psycopg2==2.7.4

