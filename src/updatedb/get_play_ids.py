from psycopg2 import pool, IntegrityError
import psycopg2
import json
import concurrent.futures
import passwords
import threading
import datetime
import time
import requests
import queue
from endpoints import MLB_API_BASE, MLB_API_VERSION

PLAY_BY_PLAY_YEAR = 1950
MLB_STATCAST_YEAR = 2015
TRIPLE_A_STATCAST_YEAR = 2023
FIRST_MINOR_LEAGUE_YEAR = 2005
GAME_TYPE_IDS = {'Spring Training': 1, 'Regular Season': 2, 'Wild Card Game': 3, 'Championship': 4, 'Division Series': 5, 'League Championship Series': 6, 'World Series': 7}
LEVEL_IDS = {'MLB': 1, 'AAA': 11, 'AA': 12, 'A+': 13, 'A': 14}

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")


PLAYER_IDS = set()
VENUE_IDS = set()
DIVISION_IDS = set()
LEAGUE_IDS = set()
TEAM_SEASONS = set()

# Load the data from the database to avoid repeat inserts
def load_data():
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT player_id FROM players")
        PLAYER_IDS = {player_id[0] for player_id in cursor.fetchall()}
        cursor.execute("SELECT venue_id FROM venues")
        VENUE_IDS = {venue_id[0] for venue_id in cursor.fetchall()}
        cursor.execute("SELECT division_id FROM divisions")
        DIVISION_IDS = {division_id[0] for division_id in cursor.fetchall()}
        cursor.execute("SELECT league_id FROM leagues")
        LEAGUE_IDS = {league_id[0] for league_id in cursor.fetchall()}
        cursor.execute("SELECT team_id, season_year FROM team_seasons")
        TEAM_SEASONS = {(team_id[0], team_id[1]) for team_id in cursor.fetchall()}
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)

def safe_float(value, default=None):
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default







    

# Process the pitch by pitch data for statcast years
def process_pitch_by_pitch_statcast(pitches, game_id, inning_half):
    inning_num = 1
    last_at_bat = 0
    at_bat_num = 0
    pitch_num = 1
    conn = None
    cursor = None
    #Iterate through the pitches
    for pitch in pitches:
        #Check if we are in a new inning or at bat
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        inning = pitch.get('inning')
        if inning != inning_num:
            inning_num = inning
            at_bat_num = 0
        
        #check if we are in a new at bat if so add it into the at bats table
        at_bat = pitch.get('ab_number')
        if at_bat != last_at_bat:
            at_bat_num += 1
            pitch_num = 1
       
        
        # Get Play_id
        play_id = pitch.get('play_id')
        print(f"Play ID: {play_id}")
        try:
            cursor.execute("""
                UPDATE pitches 
                SET play_id = %s
                WHERE game_id = %s AND inning_num = %s AND inning_half = %s AND at_bat_num = %s AND pitch_num = %s
            """, (play_id, game_id, inning_num, inning_half, at_bat_num, pitch_num))
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch data: {e}")
            if conn:
                conn.rollback()

        #Update for the next pitch so we can track where we are#
        last_at_bat = at_bat
        pitch_num += 1
    
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)

def parse_game_data(data):
    try:
        game_json = data[0]# Get the game data
        game_level = data[2]# Get the league
        game_level_id = LEVEL_IDS.get(game_level)# Get the level id
        #game_json = json.loads(game_info)# Load the JSON data into a dictionary (Turns out it loads in as a dictionary)
        
        ##########get date and time of the gam##########
        datetime_format = '%Y-%m-%dT%H:%M:%SZ'
        date_str = game_json.get('scoreboard', {}).get('datetime', {}).get('dateTime')
        game_date = datetime.datetime.strptime(date_str, datetime_format).date()
        game_year = game_date.year
        ###############################################

        ##############Get game & venue id######################
        game_id = game_json.get('scoreboard', {}).get('gamePk')
        

        if game_year >= PLAY_BY_PLAY_YEAR:
            #get pitch by pitch
            away_pitches = game_json.get('team_away', [])#this is the away team pitching
            home_pitches = game_json.get('team_home', [])#this is the home team pitching        

            # Process the pitch by pitch data for statcast years
            if (game_year >= MLB_STATCAST_YEAR and game_level_id == 1) or (game_year >= TRIPLE_A_STATCAST_YEAR and game_level_id == 11):
                process_pitch_by_pitch_statcast(away_pitches, game_id, 'B')
                process_pitch_by_pitch_statcast(home_pitches, game_id, 'T')

    except Exception as e:
        print(f"Error parsing game data: {e}")
        raise Exception(f"Error parsing game data: {str(e)}")
    else:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        try:
            game_id_str = str(game_id).zfill(6)
            
            cursor.execute("""
                DELETE FROM game_info WHERE game_id = %s
            """, (game_id_str,))
            conn.commit()
            print(f"Game {game_id_str} processed")
        except Exception as e:
            print(f"Error removing game from game_info: {e}")
            conn.rollback()
        finally:
            cursor.close()
            conn_pool.putconn(conn)
    finally:
        pass
            
        
def pull_json(batch_size=500, out_queue=None):
    # Establish the connection
    conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
    cursor = conn.cursor(name='server_cursor')  # Using server-side cursor to avoid memory issues

    # Execute the query to get the data from the game_info table
    cursor.execute("SELECT game_data, game_type, league FROM game_info ORDER BY game_date ASC limit 6000")

    # Fetch the data in batches and put each batch in the output queue for processing
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break  # Break when no more rows are available
        out_queue.put(batch)  # Put the batch in the queue
    # batch = cursor.fetchmany(batch_size)
    # out_queue.put(batch)  # Put the batch in the queue

    cursor.close()
    conn.close()
    out_queue.put(None)  # Sentinel value to indicate that no more batches are coming


def process_batches(out_queue, max_workers=48):
    while out_queue.empty():
        print("Waiting for data...")
        time.sleep(5)

    print("Got the data!")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            batch = out_queue.get()  # Get a batch from the queue
            if batch is None:  # Sentinel value indicating end of data
                break
            # Process the batch concurrently
            futures = [executor.submit(parse_game_data, game) for game in batch]
            concurrent.futures.wait(futures) # Wait for all futures to finish
    print("All done!")

def main():
    out_queue = queue.Queue(maxsize=10)  # Create a queue to pass batches between threads

    load_data() # Load the data from the database (player_ids, venue_ids, team_seasons etc)
    # Create the thread for pulling data (fetching in batches)
    batching_thread = threading.Thread(target=pull_json, args=(250, out_queue))
    batching_thread.start()

    # Start the batch processing in the main thread or with a separate pool of threads
    process_batches(out_queue)

    # Wait for the batching thread to finish (though it's finished when no more batches are in the queue)
    batching_thread.join()


if __name__ == "__main__":
    main()