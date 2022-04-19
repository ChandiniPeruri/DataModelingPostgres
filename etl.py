import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''Reads songs JSON data line by line and inserts 
    them as records into songs and artists tables.'''
    # open song file
    try:
        df = pd.read_json(filepath, lines = True)
    except:
        print("Error: Couldn't read song source file")
        print(e)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except:
        print("Error: Inserting Song table data")
        print(e)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 
                      'artist_longitude']].values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except:
        print("Error: Inserting artist table data")
        print(e)

        
def process_log_file(cur, filepath):
    '''Reads logs JSON data line by line and inserts 
    them as records into time and users tables. It
    also joins log data with songs data and inserts 
    into songplays table'''
    
    # open log file
    try:
        df = pd.read_json(filepath, lines = True)
    except:
        print("Error: Couldn't read log source file")
        print(e)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit= 'ms', infer_datetime_format=True)
    
    # insert time data records
    time_data = (t.dt.time, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, 
                 t.dt.year, t.dt.day_name())
    column_labels = ('Time', 'Hour', 'Day', 'Week', 'Month', 'Year', 'WeekDay')
    time_df = pd.DataFrame.from_dict({column_labels[i]: time_data[i] for i in range(len(time_data))})
    try:
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))
    except:
        print("Error: Inserting time data")
        print(e)
        
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
    except:
        print("Error: Inserting time data")
        print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except:
            print("Error: Selecting song data")
            print(e)
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit = 'ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data) 
        except:
            print("Error: Inserting songplay data")
            print(e)

def process_data(cur, conn, filepath, func):
    '''Gets all JSON files, loops through 
    the source JSON files and processes it.
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
    try:
        for i, datafile in enumerate(all_files, 1):
            func(cur, datafile)
            conn.commit()
            print('{}/{} files processed.'.format(i, num_files))
    except:
        print("Error: processing data")
        print(e)


def main():
    # 
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try:
        cur = conn.cursor()
    except:
        print("Error: Could not get curser to the Database")
        print(e)
    # set autocommit 
    conn.set_session(autocommit=True)
    
    # process song data
    try:
        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    except:
        print("Error: Couldn't process song data")
        print(e)
        
    # process log data
    try:
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    except:
        print("Error: Couldn't process log data")
        print(e)

    conn.close()


if __name__ == "__main__":
    main()