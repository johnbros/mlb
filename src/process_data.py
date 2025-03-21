from psycopg2 import pool
import psycopg2
import json
import concurrent.futures
import passwords
import threading
import datetime

PLAY_BY_PLAY_YEAR = 1950
MLB_STATCAST_YEAR = 2015
TRIPLE_A_STATCAST_YEAR = 2023
FIRST_MINOR_LEAGUE_YEAR = 2005


conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")






def parse_data(data):
    try:
        game_json = json.loads(data)# Load the JSON data into a dictionary
        
        #get date
        game_date = game_json.get('game_date')
        game_year = datetime.datetime.strptime(game_date, '%Y-%m-%d').year

        #get data about the teams
        away_team_data = game_json.get('away_team_data', {})
        home_team_data = game_json.get('home_team_data', {})

        #get team and player stats from the game
        away_roster = game_json.get('boxscore', {}).get('teams', {}).get('away', {}).get('players', {})
        away_stats = game_json.get('boxscore', {}).get('teams', {}).get('away', {}).get('teamStats', {})
        home_roster = game_json.get('boxscore', {}).get('teams', {}).get('home', {}).get('players', {})
        home_stats = game_json.get('boxscore', {}).get('teams', {}).get('home', {}).get('teamStats', {})

        if game_year >= PLAY_BY_PLAY_YEAR:
            #get pitch by pitch
            away_pitches = game_json.get('team_away', [])#this is the away team pitching
            home_pitches = game_json.get('team_home', [])#this is the home team pitching


        
    
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON data provided")
    except Exception as e:
        raise Exception(f"Error parsing game data: {str(e)}")



def pull_json(batch_size = 500):
    conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    cursor.execute("SELECT game_data FROM game_info ORDER BY game_date ASC") #Order by date for processing (oldest first so we capture franchise name updates)

    # Fetch the data in batches of batch_size, yield will return each batch when it is fetched. 
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        batch = [x[0] for x in batch] # Make the list 1 dimension (get ride of the tuples)
        yield batch

    cursor.close()
    conn.close()

def main():
    with concurrent.futures.ThreadPoolExecutor(max_workers=95) as executor:
        for batch in pull_json():
            executor.map(post_data, batch)



if __name__ == "__main__":
    main()