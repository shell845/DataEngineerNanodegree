#### Data Engineering Nanodegree Project 2
## Data warehouse

### Project summary
In this porject, we built an ETL pipeline which extract Sparkify's log data and song data in JSON files from S3, stage the data in Redshift, and transforms data into a set of dimensional tables for analytic purpose.

### Project structure
7 files are included in this project.

1. `dwh.cfg` is a configuration file which includes parameters of Redshift cluster, IAM role, S3 and AWS credential.

2. `sql_queries.py`includes SQL queries to parse configuration, drop tables, create tables, copy data from S3 to Redshift staging tables, select data from staging tables and insert into dimension and fact tables.

3. `create_tables.py` for table creation in Redshift.

4. `etl.py` is the script for ETL pipelining.

5. `analytic.py` is for data analytic.

6. `analytic_dashboard.ipynb` is for project testing.

7. `README.md` is the guideline for this project.


### Database schema

#### Staging Tables
* staging_events
* staging_songs

#### Fact Table
* `songplays` records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables
* `users` users in the app
   - user_id, first\_name, last\_name, gender, level
* `songs` songs in music database
   - song\_id, title, artist_id, year, duration
* `artists` artists in music database
   - artist_id, name, location, lattitude, longitude
* `time` timestamps of records in songplays broken down into specific units
   - start_time, hour, day, week, month, year, weekday

### How to run the scripts
1. Fill in `dwh.cfg`.
2. Run `create_tables.py` to create database schema.
3. Run `etl.py` to build ETL pipeline.


### Analytic dashboard
Run `analytic.py` and get following results:

```
SELECT COUNT(*) FROM staging_events
8056

SELECT COUNT(*) FROM staging_songs
14896

SELECT COUNT(*) FROM songplays
333

SELECT COUNT(*) FROM users
104

SELECT COUNT(*) FROM songs
14896

SELECT COUNT(*) FROM artists
10025

SELECT COUNT(*) FROM time
333
```

[EOD]