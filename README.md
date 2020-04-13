# SDLEP: Sparkify Data Lake ETL pipeline
SDLEP is a project for an imaginary music streaming startup called Sparkify. Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. SDLEP build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

# SDLEP files:
the SCEP project includes four files but two files are required to run the script.
* dl.cfg - Necessary - Data Lake config file. you must edit this
* etl.py - Necessary - load data from S3 into staging tables, process that data into the five fact\dimension tables and loads the data back into S3. - you must put your output data path on output_data in main function
    * Fact Table
        * songplays - records in log data associated with song plays i.e. records with page NextSong
            * songplay_id, ts, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    * Dimension Tables
        * users - users in the app
            * user_id, first_name, last_name, gender, level
        * songs - songs in music database
            * song_id, artist_id, title, duration, year
        * artists - artists in music database
            * artist_id, artist_name, location, artist_latitude, artist_longitude
        * time - timestamps of records in songplays broken down into specific units
            * start_time, hour, day, week, month, year, weekday
* README.md
* test.ipynb 

# Prerequisites
All libraries you need to install:
* pyspark.sql 
* configparser
* os
* datetime

# How to create the data lake using SDLEP:
First, we need edit dwh.cfg file.
Second, we need put our output data path in etl.py
Third, run:
###### python3 etl.py