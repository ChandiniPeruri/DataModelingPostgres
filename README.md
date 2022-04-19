# Data Modeling with Postgres
> To create Postgres database schema and ETL pipeline to analyze Sparkify song data for new music streaming app

## Table of contents
* [Overview of Project]
* [Purpose]
* [Setup]

**Overview**
> Define Fact and Dimension tables for a star schema 
> Write an ETL pipeline to transfer JSON data into Postgres using Python and SQL
> Understand what songs customers are listening to

**Pupose of Sparkify Database**
> This database is designed to optimize queries on song play analysis for analytics team. They are particularly interested in understanding what songs users are listening to.
> In order to simplify the querying process, JSON data has been loaded into Fact and Dimension tables.

**How to run Python scripts**
> Make sure you have installed python 
> To run create_tables.py file
    > open terminal, go to the file location
    > run the command "python create_tables.py"

> To run etl.py file
    > open terminal, go to the file location
    > run the command "python etl.py"

**Explanation of files in the repository** 
> \_test.ipynb_\ displays the first few rows of each table to let you check your database.
> \_create_tables.py_\ drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
> \_etl.ipynb_\ reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
> \_etl.py_\ reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
> \_sql_queries.py_\ contains all your sql queries, and is imported into the last three files above.
> \_README.md_\ provides discussion on your project.

**State and justify your database schema design and ETL pipeline ** 
>Fact Table
1. songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

>Dimension Tables
1. users - users in the app
user_id, first_name, last_name, gender, level
2. songs - songs in music database
song_id, title, artist_id, year, duration
3. artists - artists in music database
artist_id, name, location, latitude, longitude
4. time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

**Provide example queries and results for song play analysis** 
`SELECT * FROM songplays
WHERE song_id IS NOT NULL`

>Output:


