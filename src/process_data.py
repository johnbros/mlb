from psycopg2 import pool
import psycopg2
import json
import concurrent.futures
import passwords
import threading
import datetime
import requests
from endpoints import MLB_API_BASE

PLAY_BY_PLAY_YEAR = 1950
MLB_STATCAST_YEAR = 2015
TRIPLE_A_STATCAST_YEAR = 2023
FIRST_MINOR_LEAGUE_YEAR = 2005
GAME_TYPE_IDS = {'Spring Training': 1, 'Regular Season': 2, 'Wild Card Game': 3, 'Championship': 4, 'Division Series': 5, 'League Championship Series': 6, 'World Series': 7}


conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")

# Parse pitch by pitch data for statcast years
def stat_cast_pitches():
    pass

# Parse pitch by pitch data for non-statcast years
def non_stat_cast_pitches():
    pass

def get_name_and_id(type, data):
    if type == 'springVenue':
        array = 'venue'
    elif type == 'springLeague':
        array = 'league'
    else:
        array = type

    ####Get the id and name of the division, league, venue, or spring league####
    _id = data.get(f'{type}', {}).get('id')#Ex. 201
    try:
        _name = data.get(f'{type}', {}).get('name')#Ex. AL East
    except:
        try:
            response = requests.get(f"{MLB_API_BASE}{data.get(f'{type}', {}).get('link')}")
            _name = response.json().get(f'{array}s')[0].get('name')
        except:
            _name = None
    return _id, _name


# Parse & add team data to the db
def parse_team_data(team_data, year):
    #Get team id & Check if in DB#
    team_id = team_data.get('id')#Ex. 111
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    
    ####Information for the teams table####
    team_name = team_data.get('name')#Ex. Boston Red Sox
    club_name = team_data.get('clubName')#Ex. Red Sox
    team_abr = team_data.get('abbreviation')#Ex. BOS
    team_location = team_data.get('locationName')#Ex. Boston
    first_year = team_data.get('firstYearOfPlay')#Ex. 1901
    ########################################

    ####Put team data into the teams table####

    ##########Level of Play#################
    level_id = team_data.get('sport', {}).get('id')#Ex. 1
    ########################################

    ##########Division######################
    division_id, division_name = get_name_and_id('division', team_data)
    ########################################

    ##########Venue#########################
    venue_id, venue_name = get_name_and_id('venue', team_data)
    ########################################

    ##########League########################
    league_id, league_name = get_name_and_id('league', team_data)
    ########################################

    ########Spring League###################
    spring_league, spring_league_name = get_name_and_id('springLeague', team_data)
    ########################################

    ########Spring Venue####################
    spring_venue, spring_venue_name = get_name_and_id('springVenue', team_data)
    ########################################
    
    cursor.close()
    conn_pool.putconn(conn)



def parse_data(data):
    try:
        game_info = data[0]# Get the game data
        game_type = data[1]# Get the game type
        game_json = json.loads(game_info)# Load the JSON data into a dictionary
        
        ##########get date and time of the gam##########
        game_date = game_json.get('game_date')
        game_year = datetime.datetime.strptime(game_date, '%Y-%m-%d').year
        date_str = game_json.get('scoreboard', {}).get('datetime', {}).get('dateTime')
        dt = datetime.fromisoformat(date_str.replace("Z", ""))
        game_time = dt.strftime("%H:%M:%S")
        ###############################################

        ##############Get game id######################
        game_id = game_json.get('scoreboard', {}).get('gamePk')

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

            if game_year >= MLB_STATCAST_YEAR:
                pass


        
    
    except json.JSONDecodeError:
        raise ValueError("Invalid JSON data provided")
    except Exception as e:
        raise Exception(f"Error parsing game data: {str(e)}")



def pull_json(batch_size = 500):
    conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    #Get the game data and type from the game_info table (need type since its not available in the game_data)
    cursor.execute("SELECT game_data, game_type FROM game_info ORDER BY game_date ASC") #Order by date for processing (oldest first so we capture franchise name updates)

    # Fetch the data in batches of batch_size, yield will return each batch when it is fetched. 
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break # Make the list 1 dimension (get ride of the tuples)
        yield batch

    cursor.close()
    conn.close()

def main():
    # Use a thread pool to process the data
    with concurrent.futures.ThreadPoolExecutor(max_workers=95) as executor:
        for batch in pull_json():
            executor.map(post_data, batch)



if __name__ == "__main__":
    main()