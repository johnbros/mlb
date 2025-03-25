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

def get_name_and_id(type, data):
    array = 'venue' if type == 'springVenue' else 'league' if type == 'springLeague' else type

    ####Get the id and name of the division, league, venue, or spring league####
    _id = data.get(type, {}).get('id')
    _name = data.get(type, {}).get('name')
    if _id is None or _name is None:
        link = data.get(type, {}).get('link')
        if link:
            try:
                response = requests.get(f"{MLB_API_BASE}{link}")
                response.raise_for_status()  
                json_data = response.json().get(f"{array}s", [{}])[0]
                _id = _id or json_data.get('id') 
                _name = _name or json_data.get('name')
            except (requests.RequestException, IndexError, KeyError) as e:
                return None, None

    return _id, _name


# Parse & add team data to the db
def parse_team_data(team_data, year):
    #Get team id & Check if in DB#
    team_id = team_data.get('id')#Ex. 111
    conn = conn_pool.getconn()
    cursor = conn.cursor()

    try:
        ####Information for the teams table####
        team_name = team_data.get('name')#Ex. Boston Red Sox
        club_name = team_data.get('clubName')#Ex. Red Sox
        team_abr = team_data.get('abbreviation')#Ex. BOS
        team_location = team_data.get('locationName')#Ex. Boston
        first_year = team_data.get('firstYearOfPlay')#Ex. 1901
        ########################################

        ###Put team data into the teams table###
        ###Update anything thats changed since the last update###
        cursor.execute("""
            INSERT INTO teams (team_id, team_name, club_name, team_abr, team_location, first_year)
            VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (team_id) DO 
            UPDATE SET team_name = EXCLUDED.team_name,
                        club_name = EXCLUDED.club_name,
                        team_abr = EXCLUDED.team_abr,
                        team_location = EXCLUDED.team_location
            WHERE teams.team_name IS DISTINCT FROM EXCLUDED.team_name 
                        OR teams.club_name IS DISTINCT FROM EXCLUDED.club_name
                        OR teams.team_abr IS DISTINCT FROM EXCLUDED.team_abr
                        OR teams.team_location IS DISTINCT FROM EXCLUDED.team_location
        """, (team_id, team_name, club_name, team_abr, team_location, first_year))
        conn.commit()
        ########################################

        ##########Level of Play#################
        level_id = team_data.get('sport', {}).get('id')#Ex. 1
        ########################################

        ##########Division######################
        division_id, division_name = get_name_and_id('division', team_data)
        if division_id not in DIVISION_IDS:
            if division_id and division_name:
                cursor.execute("""
                    INSERT INTO divisions (division_id, division_name)
                    VALUES (%s, %s) ON CONFLICT (division_id) DO NOTHING
                """, (division_id, division_name))
            else:  
                division_id = None
            conn.commit()
            DIVISION_IDS.add(division_id)
        ########################################

        ##########Venue#########################
        venue_id, venue_name = get_name_and_id('venue', team_data)
        if venue_id not in VENUE_IDS:
            if venue_id and venue_name:
                cursor.execute("""
                    INSERT INTO venues (venue_id, venue_name)
                    VALUES (%s, %s) ON CONFLICT (venue_id) DO NOTHING
                """, (venue_id, venue_name))
            else:
                venue_id = None
            conn.commit()
            VENUE_IDS.add(venue_id)
        ########################################

        ##########League########################
        league_id, league_name = get_name_and_id('league', team_data)
        if league_id not in LEAGUE_IDS:
            if league_id and league_name:
                cursor.execute("""
                    INSERT INTO leagues (league_id, league_name)
                    VALUES (%s, %s) ON CONFLICT (league_id) DO NOTHING
                """, (league_id, league_name))
            else:
                league_id = None
            conn.commit()
            LEAGUE_IDS.add(league_id)
        ########################################

        ########Spring League###################
        spring_league_id, spring_league_name = get_name_and_id('springLeague', team_data)
        if spring_league_id not in LEAGUE_IDS:
            if spring_league_id and spring_league_name:
                cursor.execute("""
                    INSERT INTO leagues (league_id, league_name)
                    VALUES (%s, %s) ON CONFLICT (league_id) DO NOTHING
                """, (spring_league_id, spring_league_name))
            else:
                spring_league_id = None
            conn.commit()
            LEAGUE_IDS.add(spring_league_id)
        ########################################

        ########Spring Venue####################
        spring_venue_id, spring_venue_name = get_name_and_id('springVenue', team_data)
        if spring_venue_id not in VENUE_IDS:
            if spring_venue_id and spring_venue_name:
                cursor.execute("""
                    INSERT INTO venues (venue_id, venue_name)
                    VALUES (%s, %s) ON CONFLICT (venue_id) DO NOTHING
                """, (spring_venue_id, spring_venue_name))
            else:
                spring_venue_id = None
            conn.commit()
            VENUE_IDS.add(spring_venue_id)
        ########################################

        #######Insert team data into team_seasons table#####
        if (team_id, year) not in TEAM_SEASONS:
            cursor.execute("""
                INSERT INTO team_seasons (team_id, season_year, league_id, division_id, venue_id, spring_league_id, spring_venue_id, level_id, team_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (team_id, season_year) DO NOTHING
            """, (team_id, year, league_id, division_id, venue_id, spring_league_id, spring_venue_id, level_id, team_name))
            conn.commit()
            TEAM_SEASONS.add((team_id, year))
        ########################################
            

    ##Error handling##
    except IntegrityError as e:
        print(f"Database Integrity Error: {e}")
        conn.rollback()
    
    except Exception as e:
        print(f"Unexpected Error: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn_pool.putconn(conn)
    
    return team_id

# Inserts game information into the games table
def post_games(h_team_id, a_team_id, venue_id, level_id, type_id, date, game_id, game_time, weather, wind, season_year):
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        #Im not expecting on conflict to occur but in case it does i'll just have it do nothing
        cursor.execute("""
            INSERT INTO games (game_id, date, game_time, venue_id, level_id, type_id, h_team_id, a_team_id, weather, wind, season_year)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING
        """, (game_id, date, game_time, venue_id, level_id, type_id, h_team_id, a_team_id, weather, wind, season_year))
        conn.commit()
    except Exception as e:
        print(f"Error inserting game data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)

# Returns the wind and weatherif they exist otherwise returns none for the categories that dont exist
def parse_info_array(info_array):
    wind = None
    weather = None
    if info_array:
        for info in info_array:
            info_label = info.get('label')
            if info_label == 'Weather':
                weather = info.get('value')
            elif info_label == 'Wind':
                wind = info.get('value')
    return wind, weather

# Parses the team stats and inserts them into the team_games table
def parse_team_stats(stats, team_id, game_id):
    team_batting = stats.get('batting')
    team_pitching = stats.get('pitching')
    team_fielding = stats.get('fielding')

    # Batting Stats
    runs_scored = team_batting.get('runs')
    hits = team_batting.get('hits')
    rbis = team_batting.get('rbi')
    home_runs = team_batting.get('homeRuns')
    doubles = team_batting.get('doubles')
    triples = team_batting.get('triples')
    walks = team_batting.get('walks')
    strikeouts = team_batting.get('strikeOuts')

    # Pitching Stats
    earned_runs_allowed = team_pitching.get('earnedRuns')
    runs_allowed = team_pitching.get('runs')
    innings_pitched = safe_float(team_pitching.get('inningsPitched'))
    total_pitches_thrown = team_pitching.get('numberOfPitches')
    struck_out = team_pitching.get('strikeOuts')
    walked = team_pitching.get('walks')
    hits_allowed = team_pitching.get('hits')
    home_runs_allowed = team_pitching.get('homeRuns')
    doubles_allowed = team_pitching.get('doubles')
    triples_allowed = team_pitching.get('triples')

    # Fielding Stats
    errors = team_fielding.get('errors')

    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        ##Insert team stats into the team_games table##
        cursor.execute("""
            INSERT INTO team_games (game_id, team_id, runs_scored, hits, errors, rbis, home_runs, doubles, triples, walks, strikeouts, earned_runs_allowed, runs_allowed, innings_pitched, total_pitches_thrown, struck_out, walked, hits_allowed, home_runs_allowed, doubles_allowed, triples_allowed)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, team_id) DO NOTHING
        """, (game_id, team_id, runs_scored, hits, errors, rbis, home_runs, doubles, triples, walks, strikeouts, earned_runs_allowed, runs_allowed, innings_pitched, total_pitches_thrown, struck_out, walked, hits_allowed, home_runs_allowed, doubles_allowed, triples_allowed))
        conn.commit()
    except Exception as e:
        print(f"Error inserting team stats: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)

# Parses the positions so that no unexpected ones get past me
def parse_position(position_data):
    position_id = position_data.get('code')
    position_name = position_data.get('name')
    if position_id == 'O':
        position_id = 12
    elif not position_id:
        position_id = -1
        position_name = 'Unknown'
    else:
        position_id = int(position_id)
        position_name = position_data.get('name')
    conn = None
    cursor = None
    try:
        conn = conn_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO positions (position_id, position_name)
            VALUES (%s, %s) ON CONFLICT (position_id) DO NOTHING
        """, (position_id, position_name))
        conn.commit()
    except Exception as e:
        print(f"Error inserting position data: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn_pool.putconn(conn)
    return position_id


# Puts a player into the players table
def post_player(player_data, game_id):
    link = player_data.get('link')
    player_id = player_data.get('id')
    conn = None
    cursor = None
    if player_id not in PLAYER_IDS:
        if player_id == 0:
            player_name = 'Unknown'
            try:
                conn = conn_pool.getconn()
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO players (player_id, player_name)
                    VALUES (%s, %s) ON CONFLICT (player_id) DO NOTHING
                """, (player_id, player_name))
                conn.commit()
                PLAYER_IDS.add(player_id)
            except Exception as e:
                print(f"Error inserting player data: {e}")
                if conn:
                    conn.rollback()
            finally:
                if cursor:
                    cursor.close()
                if conn:
                    conn_pool.putconn(conn)
            return player_id
        player_name = player_data.get('fullName')
        try: 
            # Call the api and get the players information
            response = requests.get(f"{MLB_API_BASE}{link}")
            response.raise_for_status()
            json_data = response.json().get('people', [{}])[0]
            player_id = player_id or json_data.get('id') 
            player_name = player_name or json_data.get('fullName')
            player_birthday = json_data.get('birthDate')
            position_data = json_data.get('primaryPosition', {})
            position_id = parse_position(position_data)
            bat_side = json_data.get('batSide', {}).get('code')
            throw_side = json_data.get('pitchHand', {}).get('code')
            sz_top = json_data.get('strikeZoneTop')
            sz_bot = json_data.get('strikeZoneBottom')
            conn = conn_pool.getconn()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO players (player_id, player_name, player_birthday, position_id, bat_side, throw_side, sz_top, sz_bot)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (player_id) DO UPDATE
                SET player_name = EXCLUDED.player_name WHERE players.player_name IS DISTINCT FROM EXCLUDED.player_name
            """, (player_id, player_name, player_birthday, position_id, bat_side, throw_side, sz_top, sz_bot))
            conn.commit()
            PLAYER_IDS.add(player_id)
        except (requests.RequestException, IndexError, KeyError) as e:
            print(f"Error getting player data: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:    
                conn_pool.putconn(conn)

    return player_id

# Posts a players pitching stats
def post_pitching_stats(player_id, game_id, team_id, pitching_stats):
    innings_pitched = pitching_stats.get('inningsPitched')
    batters_faced = pitching_stats.get('battersFaced')
    earned_runs = pitching_stats.get('earnedRuns')
    runs_allowed = pitching_stats.get('runs')
    hits_allowed = pitching_stats.get('hits')
    strikeouts = pitching_stats.get('strikeOuts')
    walks = pitching_stats.get('baseOnBalls')
    home_runs_allowed = pitching_stats.get('homeRuns')
    hit_by_pitch = pitching_stats.get('hitByPitch')
    wild_pitches = pitching_stats.get('wildPitches')
    balks = pitching_stats.get('balks')
    pickoffs = pitching_stats.get('pickoffs')
    complete_game = bool(pitching_stats.get('completeGame')) 
    shutout = bool(pitching_stats.get('shutout')) 
    save_opportunity = bool(pitching_stats.get('saveOpportunities')) 
    inherited_runners = pitching_stats.get('inheritedRunners')
    inherited_runners_scored = pitching_stats.get('inheritedRunnersScored')
    pitches_thrown = pitching_stats.get('pitchesThrown')
    strikes_thrown = pitching_stats.get('strikes')
    balls_thrown = pitching_stats.get('balls')
    save = bool(pitching_stats.get('save')) 
    doubles_allowed = pitching_stats.get('doubles')
    triples_allowed = pitching_stats.get('triples')
    air_outs = pitching_stats.get('airOuts')
    line_outs = pitching_stats.get('lineOuts')
    fly_outs = pitching_stats.get('flyOuts')
    pop_outs = pitching_stats.get('popOuts')
    ground_outs = pitching_stats.get('groundOuts') 
    win = bool(pitching_stats.get('win')) 
    loss = bool(pitching_stats.get('loss'))
    hold = bool(pitching_stats.get('hold')) 
    game_started = bool(pitching_stats.get('gamesStarted')) 
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO player_games_pitching (game_id, team_id, player_id, innings_pitched, batters_faced, earned_runs, runs_allowed, hits_allowed, strikeouts, walks, home_runs_allowed, hit_by_pitch, wild_pitches, balks, pickoffs, complete_game, shutout, save_opportunity, inherited_runners, inherited_runners_scored, pitches_thrown, strikes_thrown, balls_thrown, save, doubles_allowed, triples_allowed, air_outs, line_outs, fly_outs, pop_outs, ground_outs, win, loss, hold, game_started)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, team_id, player_id) DO NOTHING
        """, (game_id, team_id, player_id, innings_pitched, batters_faced, earned_runs, runs_allowed, hits_allowed, strikeouts, walks, home_runs_allowed, hit_by_pitch, wild_pitches, balks, pickoffs, complete_game, shutout, save_opportunity, inherited_runners, inherited_runners_scored, pitches_thrown, strikes_thrown, balls_thrown, save, doubles_allowed, triples_allowed, air_outs, line_outs, fly_outs, pop_outs, ground_outs, win, loss, hold, game_started))
        conn.commit()
    except Exception as e:
        print(f"Error inserting pitching stats: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)



# Posts a players hitting stats
def post_hitting_stats(player_id, game_id, team_id, position_id, hitting_stats):
    at_bats = hitting_stats.get('atBats')
    hits = hitting_stats.get('hits')
    walks = hitting_stats.get('walks')
    total_bases = hitting_stats.get('totalBases')
    rbis = hitting_stats.get('rbi')
    hbp = hitting_stats.get('hitByPitch')
    hrs = hitting_stats.get('homeRuns')
    doubles = hitting_stats.get('doubles')
    triples = hitting_stats.get('triples')
    strikeouts = hitting_stats.get('strikeOuts')
    plate_appearances = hitting_stats.get('plateAppearances')
    runs = hitting_stats.get('runs')
    stolen_bases = hitting_stats.get('stolenBases')
    caught_stealing = hitting_stats.get('caughtStealing')
    ground_into_double_play = hitting_stats.get('groundIntoDoublePlay')
    sac_bunts = hitting_stats.get('sacBunts')
    sac_flies = hitting_stats.get('sacFlies')
    left_on_base = hitting_stats.get('leftOnBase')
    ground_into_triple_play = hitting_stats.get('groundIntoTriplePlay')
    line_outs = hitting_stats.get('lineOuts')
    pop_outs = hitting_stats.get('popOuts')
    fly_outs = hitting_stats.get('flyOuts')
    air_outs = hitting_stats.get('airOuts')
    ground_outs = hitting_stats.get('groundOuts')
    catchers_interference = hitting_stats.get('catchersInterference')
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO player_games_hitting (game_id, team_id, player_id, position_id, at_bats, hits, walks, total_bases, rbis, hbp, hrs, doubles, triples, strikeouts, plate_appearances, runs, stolen_bases, caught_stealing, ground_into_double_play, sac_bunts, sac_flies, left_on_base, ground_into_triple_play, line_outs, pop_outs, fly_outs, air_outs, ground_outs, catchers_interference)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, team_id, player_id) DO NOTHING
        """, (game_id, team_id, player_id, position_id, at_bats, hits, walks, total_bases, rbis, hbp, hrs, doubles, triples, strikeouts, plate_appearances, runs, stolen_bases, caught_stealing, ground_into_double_play, sac_bunts, sac_flies, left_on_base, ground_into_triple_play, line_outs, pop_outs, fly_outs, air_outs, ground_outs, catchers_interference))
        conn.commit()
    except Exception as e:
        print(f"Error inserting hitting stats: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)
    

# Posts a players fielding stats
def post_fielding_stats(player_id, game_id, team_id, position_id, fielding_stats):
    putouts = fielding_stats.get('putOuts')
    assists = fielding_stats.get('assists')
    errors = fielding_stats.get('errors')
    caught_stealing = fielding_stats.get('caughtStealing')
    passed_balls = fielding_stats.get('passedBalls')
    stolen_bases_allowed = fielding_stats.get('stolenBases')
    pickoffs = fielding_stats.get('pickoffs')
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO player_games_fielding (game_id, team_id, player_id, position_id, putouts, assists, errors, caught_stealing, passed_balls, stolen_bases_allowed, pickoffs)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, team_id, player_id) DO NOTHING
        """, (game_id, team_id, player_id, position_id, putouts, assists, errors, caught_stealing, passed_balls, stolen_bases_allowed, pickoffs))
        conn.commit()
    except Exception as e:
        print(f"Error inserting fielding stats: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)



# Posts a players presence on the roster
def post_roster_games(game_id, team_id, player_id):
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO roster_games (game_id, team_id, player_id)
            VALUES (%s, %s, %s) ON CONFLICT (game_id, team_id, player_id) DO NOTHING
        """, (game_id, team_id, player_id))
        conn.commit()
    except Exception as e:
        print(f"Error inserting roster data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)

def get_position_id(player_id):
    response = requests.get(f"{MLB_API_BASE}/{MLB_API_VERSION}/people/{player_id}")
    response.raise_for_status()
    json_data = response.json().get('people', [{}])[0]
    position_data = json_data.get('primaryPosition', {})
    return parse_position(position_data)


# Parses the players stats and inserts them into roster_games, player_games_(hitting, pitching, fielding) tables (if they recorded stats in those categories)
def parse_players(players, team_id, game_id):
    #iterate through players to get each players stats for the game
    for x in players:
        player = players.get(x, {})
        player_id = post_player(player.get('person', {}), game_id)
        post_roster_games(game_id, team_id, player_id)
        player_stats = player.get('stats', {})
        player_position_id = parse_position(player.get('position', {}))
        if not player_position_id:
            player_position_id = get_position_id(player_id)
        hitting_stats = player_stats.get('batting', {})
        pitching_stats = player_stats.get('pitching', {})
        fielding_stats = player_stats.get('fielding', {})

        if hitting_stats:
            post_hitting_stats(player_id, game_id, team_id, player_position_id, hitting_stats)
        if pitching_stats:
            post_pitching_stats(player_id, game_id, team_id, pitching_stats)
        if fielding_stats:
            post_fielding_stats(player_id, game_id, team_id, player_position_id, fielding_stats)

# Processes the innings data
def process_innings(innings, game_id):
    conn = None
    cursor = None
    for inning in innings:
        try:
            conn = conn_pool.getconn()
            cursor = conn.cursor()
            inning_num = inning.get('num')
            inning_home = inning.get('home', {})
            inning_away = inning.get('away', {})

            #######Home Team/Bottom Half#######
            home_hits = inning_home.get('hits')
            home_runs = inning_home.get('runs')
            away_errors = inning_away.get('errors')
            home_lob = inning_home.get('leftOnBase')
            home_half = 'B'
            cursor.execute("""
                INSERT INTO innings (game_id, inning_num, inning_half, hits, runs_scored, errors, left_on_base)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning_num, inning_half) DO NOTHING
            """, (game_id, inning_num, home_half, home_hits, home_runs, away_errors, home_lob))
            ###################################
            #######Away Team/Top Half##########
            away_hits = inning_away.get('hits')
            away_runs = inning_away.get('runs')
            home_errors = inning_home.get('errors')
            away_lob = inning_away.get('leftOnBase')
            away_half = 'T'
            cursor.execute("""
                INSERT INTO innings (game_id, inning_num, inning_half, hits, runs_scored, errors, left_on_base)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning_num, inning_half) DO NOTHING
            """, (game_id, inning_num, away_half, away_hits, away_runs, home_errors, away_lob))
            ###################################
            conn.commit()

        except Exception as e:
            print(f"Error inserting inning data: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn_pool.putconn(conn)


    


# Process the pitch by pitch data
def process_pitch_by_pitch_pre_statcast(pitches, game_id, inning_half):
    inning_num = 1
    last_at_bat = 1
    at_bat_num = 1
    pitch_num = 1
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    #Iterate through the pitches
    for pitch in pitches:
        #Check if we are in a new inning or at bat
        inning = pitch.get('inning')
        if inning != inning_num:
            inning_num = inning
            at_bat_num = 1
        
        #check if we are in a new at bat if so add it into the at bats table
        at_bat = pitch.get('ab_number')
        if at_bat != last_at_bat:
            at_bat_num += 1
            pitch_num = 1

            #Get the batter and the pitcher
            batter_id = pitch.get('batter')
            pitcher_id = pitch.get('pitcher')
            
            #Get at bat outcome result
            ab_outcome_label = pitch.get('result')

            try:
                #Insert ab_outcome_label
                cursor.execute("""
                    INSERT INTO ab_outcomes (ab_outcome_label)
                    VALUES (%s) ON CONFLICT (ab_outcome_label) DO NOTHING
                    RETURNING ab_outcome_id
                """, (ab_outcome_label,))

                #Get at bat outcome id
                ab_outcome_id = cursor.fetchone()[0]
                conn.commit()

                #Get the description of the at bat result
                at_bat_des = pitch.get('des')

                cursor.execute("""
                    INSERT INTO at_bats (game_id, inning_num, inning_half, at_bat_num, batter_id, pitcher_id, ab_outcome_id, at_bat_des)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning, inning_half, at_bat_num) DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, batter_id, pitcher_id, ab_outcome_id, at_bat_des))
                conn.commit()

            except Exception as e:
                print(f"Error inserting at bat data: {e}")
                conn.rollback()

        #Get information about the pitch outcome
        p_call = pitch.get('call')
        p_call_name = pitch.get('call_name')
        p_description = pitch.get('description')
        p_result_code = pitch.get('result_code')
        try: 
            #Insert pitch data
            cursor.execute("""
                INSERT INTO pitch_outcomes (p_call, p_call_name, p_description, p_result_code)
                VALUES (%s, %s, %s, %s) ON CONFLICT (p_call, p_call_name, p_description, p_result_code) DO NOTHING
                RETURNING p_outcome_id
            """, (p_call, p_call_name, p_description, p_result_code))

            #Get the outcome_id
            p_outcome_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch data: {e}")
            conn.rollback()


        balls = pitch.get('balls')
        strikes = pitch.get('strikes')
        outs = pitch.get('outs')

        #Insert pitch into the pitches table
        try:
            cursor.execute("""
                INSERT INTO pitches (game_id, inning_num, inning_half, at_bat_num, pitch_num, pitch_outcome_id, balls, strikes, outs)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning_num, inning_half, at_bat_num, pitch_num) DO NOTHING
            """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, p_outcome_id, balls, strikes, outs))
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch data: {e}")
            conn.rollback()



        #Update for the next pitch so we can track where we are#
        last_at_bat = at_bat_num
        pitch_num += 1
    

    cursor.close()
    conn_pool.putconn(conn)

# Process the pitch by pitch data for statcast years
def process_pitch_by_pitch_statcast(pitches, game_id, inning_half):
    inning_num = 1
    last_at_bat = 1
    at_bat_num = 1
    pitch_num = 1
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    #Iterate through the pitches
    for pitch in pitches:
        #Check if we are in a new inning or at bat
        inning = pitch.get('inning')
        if inning != inning_num:
            inning_num = inning
            at_bat_num = 1
        
        #check if we are in a new at bat if so add it into the at bats table
        at_bat = pitch.get('ab_number')
        if at_bat != last_at_bat:
            at_bat_num += 1
            pitch_num = 1

            #Get the batter and the pitcher
            batter_id = pitch.get('batter')
            pitcher_id = pitch.get('pitcher')
            
            #Get at bat outcome result
            ab_outcome_label = pitch.get('result')
            ab_outcome_id = None
            try:
                #Insert ab_outcome_label
                cursor.execute("""
                    INSERT INTO ab_outcomes (ab_outcome_label)
                    VALUES (%s) ON CONFLICT (ab_outcome_label) DO NOTHING
                    RETURNING ab_outcome_id
                """, (ab_outcome_label,))

                #Get at bat outcome id
                ab_outcome_id = cursor.fetchone()[0]
                conn.commit()

                #Get the description of the at bat result
                at_bat_des = pitch.get('des')
                cursor.execute("""
                    INSERT INTO at_bats (game_id, inning_num, inning_half, at_bat_num, batter_id, pitcher_id, ab_outcome_id, at_bat_des)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning, inning_half, at_bat_num) DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, batter_id, pitcher_id, ab_outcome_id, at_bat_des))
                conn.commit()

            except Exception as e:
                print(f"Error inserting at bat data: {e}")
                conn.rollback()

        #Get information about the pitch outcome
        p_call = pitch.get('call')
        p_call_name = pitch.get('call_name')
        p_description = pitch.get('description')
        p_result_code = pitch.get('result_code')
        p_outcome_id = None
        try: 
            #Insert pitch data
            cursor.execute("""
                INSERT INTO pitch_outcomes (p_call, p_call_name, p_description, p_result_code)
                VALUES (%s, %s, %s, %s) ON CONFLICT (p_call, p_call_name, p_description, p_result_code) DO NOTHING
                RETURNING p_outcome_id
            """, (p_call, p_call_name, p_description, p_result_code))

            #Get the outcome_id
            p_outcome_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch data: {e}")
            conn.rollback()

        # Balls, Strikes, and Outs
        balls = pitch.get('balls')
        strikes = pitch.get('strikes')
        outs = pitch.get('outs')

        #Get pitch type data
        pitch_type_code = pitch.get('pitch_type')
        pitch_type_name = pitch.get('pitch_name')
        pitch_type_id = None
        try:
            cursor.execute("""
                INSERT INTO pitch_types (pitch_type_code, pitch_type_name)
                VALUES (%s, %s) ON CONFLICT (pitch_type_code, pitch_type_name) DO NOTHING
                RETURNING pitch_type_id
            """, (pitch_type_code, pitch_type_name))
            pitch_type_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch type data: {e}")
            conn.rollback()
        
        #Get the pitch statcast data
        initial_speed = safe_float(pitch.get('start_speed'))
        final_speed = safe_float(pitch.get('end_speed'))
        x0 = safe_float(pitch.get('x0'))
        y0 = safe_float(pitch.get('y0'))
        z0 = safe_float(pitch.get('z0'))
        vx0 = safe_float(pitch.get('vx0'))
        vy0 = safe_float(pitch.get('vy0'))
        vz0 = safe_float(pitch.get('vz0'))
        ax = safe_float(pitch.get('ax'))
        ay = safe_float(pitch.get('ay'))
        az = safe_float(pitch.get('az'))
        px = safe_float(pitch.get('px'))
        pz = safe_float(pitch.get('pz'))
        spin_rate = safe_float(pitch.get('spin_rate'))
        sz_top = safe_float(pitch.get('sz_top'))
        sz_bot = safe_float(pitch.get('sz_bot'))
        pfxx = safe_float(pitch.get('pfxX'))
        pfzz = safe_float(pitch.get('pfxZ'))
        pfxZWithGravity = safe_float(pitch.get('pfxZWithGravity'))
        pfxXWithGravity = safe_float(pitch.get('pfxXWithGravity'))
        extension = safe_float(pitch.get('extension'))
        breakX = safe_float(pitch.get('breakX'))
        breakZ = safe_float(pitch.get('breakZ'))

        #Insert pitch statcast into pitch_data table
        pitch_data_id = None
        try:
            cursor.execute("""
                INSERT INTO pitch_data (pitch_type_id, initial_speed, final_speed, x0, y0, z0, vx0, vy0, vz0, ax, ay, az, px, pz, spin_rate, sz_top, sz_bot, pfxx, pfzz, pfxZWithGravity, pfxXWithGravity, extension, breakX, breakZ)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING pitch_data_id
            """, (pitch_type_id, initial_speed, final_speed, x0, y0, z0, vx0, vy0, vz0, ax, ay, az, px, pz, spin_rate, sz_top, sz_bot, pfxx, pfzz, pfxZWithGravity, pfxXWithGravity, extension, breakX, breakZ))
            pitch_data_id = cursor.fetchone()[0]
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch statcast data: {e}")
            conn.rollback()


        #Insert pitch into the pitches table
        try:
            cursor.execute("""
                INSERT INTO pitches (game_id, inning_num, inning_half, at_bat_num, pitch_num, pitch_outcome_id, balls, strikes, outs, pitch_data_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id, inning_num, inning_half, at_bat_num, pitch_num) DO NOTHING
            """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, p_outcome_id, balls, strikes, outs, pitch_data_id))
            conn.commit()
        except Exception as e:
            print(f"Error inserting pitch data: {e}")
            conn.rollback()

        #Most consistent statcast hit stat
        hit_speed = safe_float(pitch.get('hit_speed'))

        #If the ball was hit into play
        if hit_speed:
            #Get the statcast data 
            hit_angle = safe_float(pitch.get('hit_angle'))
            isbarrel = pitch.get('is_barrel')
            bat_speed = safe_float(pitch.get('batSpeed'))
            distance = safe_float(pitch.get('hit_distance'))
            hc_y_ft = safe_float(pitch.get('hc_y_ft'))
            hc_x_ft = safe_float(pitch.get('hc_x_ft'))
            
            #Insert hit data into bip_data table
            bip_data_id = None
            try:
                cursor.execute("""
                    INSERT INTO bip_data (pitch_data_id, hit_speed, hit_angle, isbarrel, bat_speed, distance, hc_y_ft, hc_x_ft)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING bip_data_id
                """, (pitch_data_id, hit_speed, hit_angle, isbarrel, bat_speed, distance, hc_y_ft, hc_x_ft))
                bip_data_id = cursor.fetchone()[0]
                conn.commit()

                cursor.execute("""
                    UPDATE at_bats SET bip_data_id = %s WHERE game_id = %s AND inning_num = %s AND inning_half = %s AND at_bat_num = %s
                """, (bip_data_id, game_id, inning_num, inning_half, at_bat_num))
                conn.commit()

            except Exception as e:
                print(f"Error inserting bip data: {e}")
                conn.rollback()

        #Update for the next pitch so we can track where we are#
        last_at_bat = at_bat_num
        pitch_num += 1
    

    cursor.close()
    conn_pool.putconn(conn)

def parse_game_data(data):
    try:
        game_json = data[0]# Get the game data
        game_type = data[1]# Get the game type
        game_type_id = GAME_TYPE_IDS.get(game_type)# Get the game type id
        game_level = data[2]# Get the league
        game_level_id = LEVEL_IDS.get(game_level)# Get the level id
        #game_json = json.loads(game_info)# Load the JSON data into a dictionary (Turns out it loads in as a dictionary)
        
        ##########get date and time of the gam##########
        datetime_format = '%Y-%m-%dT%H:%M:%SZ'
        date_str = game_json.get('scoreboard', {}).get('datetime', {}).get('dateTime')
        game_time = datetime.datetime.strptime(date_str, datetime_format).time()
        game_date = datetime.datetime.strptime(date_str, datetime_format).date()
        game_year = game_date.year
        ###############################################

        ##############Get game & venue id######################
        game_id = game_json.get('scoreboard', {}).get('gamePk')
        venue_id = game_json.get('venue_id')
        #######################################################

        ##############Info Array#######################
        info_array = game_json.get('boxscore', {}).get('info')
        wind, weather = parse_info_array(info_array) #Get the wind and weather
        ###############################################
        

        #get data about the teams
        away_team_data = game_json.get('away_team_data', {})
        away_team_id = parse_team_data(away_team_data, game_year)#Inserts away team data into the teams table and returns the id
        home_team_data = game_json.get('home_team_data', {})
        home_team_id = parse_team_data(home_team_data, game_year)#Inserts home team data into the teams table and returns the id

        #insert game data into the games table
        post_games(home_team_id, away_team_id, venue_id, game_level_id, game_type_id, game_date, game_id, game_time, weather, wind, game_year)
        
        #Get team stats from the game and put them into the db
        away_stats = game_json.get('boxscore', {}).get('teams', {}).get('away', {}).get('teamStats', {})
        home_stats = game_json.get('boxscore', {}).get('teams', {}).get('home', {}).get('teamStats', {})
        
        parse_team_stats(away_stats, away_team_id, game_id) #Insert away team stats into the team_games table
        parse_team_stats(home_stats, home_team_id, game_id) #Insert home team stats into the team_games table

        #Get the players stats and insert them into their tables
        away_roster = game_json.get('boxscore', {}).get('teams', {}).get('away', {}).get('players', {})
        parse_players(away_roster, away_team_id, game_id)
        home_roster = game_json.get('boxscore', {}).get('teams', {}).get('home', {}).get('players', {})
        parse_players(home_roster, home_team_id, game_id)

        game_innings = game_json.get('scoreboard', {}).get('linescore', {}).get('innings', [])
        process_innings(game_innings, game_id)

        if game_year >= PLAY_BY_PLAY_YEAR:
            #get pitch by pitch
            away_pitches = game_json.get('team_away', [])#this is the away team pitching
            home_pitches = game_json.get('team_home', [])#this is the home team pitching        
            

            # Process the pitch by pitch data for statcast years
            if (game_year >= MLB_STATCAST_YEAR and game_level_id == 1) or (game_year >= TRIPLE_A_STATCAST_YEAR and game_level_id == 11):
                process_pitch_by_pitch_statcast(away_pitches, game_id, 'B')
                process_pitch_by_pitch_statcast(home_pitches, game_id, 'T')
            else:
                process_pitch_by_pitch_pre_statcast(away_pitches, game_id, 'B')
                process_pitch_by_pitch_pre_statcast(home_pitches, game_id, 'T')

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
    cursor.execute("SELECT game_data, game_type, league FROM game_info ORDER BY game_date ASC")

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


def process_batches(out_queue, max_workers=95):
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
    out_queue = queue.Queue()  # Create a queue to pass batches between threads

    load_data() # Load the data from the database (player_ids, venue_ids, team_seasons etc)
    # Create the thread for pulling data (fetching in batches)
    batching_thread = threading.Thread(target=pull_json, args=(500, out_queue))
    batching_thread.start()

    # Start the batch processing in the main thread or with a separate pool of threads
    process_batches(out_queue)

    # Wait for the batching thread to finish (though it's finished when no more batches are in the queue)
    batching_thread.join()


if __name__ == "__main__":
    main()