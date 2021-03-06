import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Read the song data files row by row,
    filter out needed columns and insert into songs and artists tables in Sparkify DB.
        
        Parameters:
            cur (psycopg2.connect().cursor()): cursor of Sparkify DB
            filepath: filepath of song dataset
            
        Returns: N/A
    '''
    
    # open song file
    df = pd.read_json(filepath, lines=True)

    artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = df.values[0]
    
    # insert song record
    song_data = [song_id, title, artist_id, year, duration]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [artist_id, artist_name, artist_location, artist_latitude, artist_longitude]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Read the log data files row by row,
    filter out needed columns and insert into songplays, users and time tables in Sparkify DB.
        
        Parameters:
            cur (psycopg2.connect().cursor()): cursor of Sparkify DB
            filepath: filepath of log dataset
            
        Returns: N/A
    '''
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = []
    for data in t:
        time_data.append([data, data.hour, data.day, data.week, data.month, data.year, data.day_name()])

    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_records(time_data, columns = column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), int(row.userId), row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Retrieve all nested dataset files under provided filepath,
    call corresponding functions for handling.
        
        Parameters:
            conn (psycopg2.connect()): connection of Sparkify DB
            cur (conn.cursor()): cursor of Sparkify DB
            filepath: filepath of dataset
            func: functions to process the dataset files
            
        Returns: N/A
    '''
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    The main function to be called to extract data from JSON dataset files, 
    transform data and load to PostgreSQL DB Sparkify
    
    Workflow:
        1. Create connection with Sparkify DB
        2. Create DB cursor
        3. Process dataset
        4. Close connection
    '''
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()