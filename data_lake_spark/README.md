#### Data Engineering Nanodegree Project 3
# Data Lake

## Project summary
In this project, we build an ETL pipeline for data lake.

ETL pipeline flow:

1. Extract data from S3
2. Process with Spark
3. Load back processed data as fact and dimensional tables to S3

## Project structure
2 files are included in this project:

1. `etl.py` is the script to conduct data lake ETL

	```
	[KEYS]
	AWS_ACCESS_KEY_ID=<your_id>
	AWS_SECRET_ACCESS_KEY=<your_key>
	```

2. `dl.cfg` is the configuration file

## Data set
1. `s3://udacity-dend/song_data`: song data contains information of songs and artists
2. `s3://udacity-dend/log_data`: user event logs

## Database schema

### Fact Table
- `songplays` records in log data associated with song plays
	- songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent

### Dimension Tables
- `users` users in the app
	- user_id, first_name, last_name, gender, level

- `songs` songs in music database
	- song_id, title, artist_id, year, duration

- `artists` artists in music database
	- artist_id, name, location, lattitude, longitude

- `time` timestamps of records in songplays broken down into specific units
	- start_time, hour, day, week, month, year, weekday

## How to run the scripts
1. Fill in `dl.cfg`
2. Run `etl.py` to conduct ETL

## Analytics
Self define any queries to run

[EOF]