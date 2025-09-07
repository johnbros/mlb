import psycopg2
from psycopg2 import pool
from passwords import password
import requests
import json
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import re
import pytz
import sys
from getStartTime import get_start_times

def extract_time(raw_string):
    # Extract the time using regex (handles "a.m." / "p.m." / "am" / "pm")
    match = re.search(r"(\d{1,2}:\d{2})\s*(a\.m\.|p\.m\.|am|pm)", raw_string.lower())
    if match:
        time_str = match.group(1)
        meridian = match.group(2).replace(".", "")  # remove dots
        return f"{time_str} {meridian}"
    return None


conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{password}", host="127.0.0.1", port="5432")
failed_game_ids = set()


VENUE_TZ_MAP = {
    "Anaheim Stadium": "America/Los_Angeles",
    "Angel Stadium of Anaheim": "America/Los_Angeles",
    "AT&T Park": "America/Los_Angeles",  # Former name of Oracle Park
    "Ameriquest Field": "America/Chicago",  # Former name of Globe Life Park
    "Bank One Ballpark": "America/Phoenix",
    "Busch Stadium": "America/Chicago",
    "Busch Stadium I": "America/Chicago",
    "Citi Field": "America/New_York",
    "Citizens Bank Park": "America/New_York",
    "Comerica Park": "America/Detroit",
    "Comiskey Park": "America/Chicago",
    "Coors Field": "America/Denver",
    "D.C. Stadium": "America/New_York",
    "Dodger Stadium": "America/Los_Angeles",
    "Dolphin Stadium": "America/New_York",  # Now Hard Rock Stadium, in Miami
    "Dunn Tire Park": "America/New_York",  # Buffalo, NY
    "Enron Field": "America/Chicago",  # Became Minute Maid Park
    "Estadio Alfredo Harp Helu": "America/Mexico_City",
    "Estadio de Beisbol Monterrey": "America/Monterrey",
    "Fenway Park": "America/New_York",
    "Field of Dreams": "America/Chicago",
    "Fort Bragg Field": "America/New_York",
    "George M. Steinbrenner Field": "America/New_York",
    "Globe Life Field": "America/Chicago",
    "Great American Ball Park": "America/New_York",
    "Gocheok Sky Dome": "Asia/Seoul",
    "Hiram Bithorn Stadium": "America/Puerto_Rico",
    "Hubert H. Humphrey Metrodome": "America/Chicago",
    "Jacobs Field": "America/New_York",
    "Joe Robbie Stadium": "America/New_York",
    "Journey Bank Ballpark": "America/New_York",  # Trenton Thunder's park
    "Kauffman Stadium": "America/Chicago",
    "London Stadium": "Europe/London",
    "Marlins Park": "America/New_York",
    "Minute Maid Park": "America/Chicago",
    "Miller Park": "America/Chicago",  # Became American Family Field
    "Nationals Park": "America/New_York",
    "Oakland-Alameda County Coliseum": "America/Los_Angeles",
    "Oriole Park at Camden Yards": "America/New_York",
    "PacBell Park": "America/Los_Angeles",
    "PETCO Park": "America/Los_Angeles",
    "PNC Park": "America/New_York",
    "Raley Field": "America/Los_Angeles",
    "Rickwood Field": "America/Chicago",
    "Royals Stadium": "America/Chicago",
    "Safeco Field": "America/Los_Angeles",
    "Shea Stadium": "America/New_York",
    "SkyDome": "America/Toronto",
    "SunTrust Park": "America/New_York",
    "Sydney Cricket Ground": "Australia/Sydney",
    "Target Field": "America/Chicago",
    "TD Ameritrade Park": "America/Chicago",
    "TD Ballpark": "America/New_York",
    "The Ballpark at Arlington": "America/Chicago",
    "The Stadium at the ESPN Wide World of Sports": "America/New_York",
    "Tokyo Dome": "Asia/Tokyo",
    "Tropicana Field": "America/New_York",
    "Turner Field": "America/New_York",
    "U.S. Cellular Field": "America/Chicago",  # Now Guaranteed Rate Field
    "Weeghman Park": "America/Chicago",
    "Wrigley Field": "America/Chicago",
    "Yankee Stadium": "America/New_York",
    "Yankee Stadium I": "America/New_York",
    "Chase Field": "America/Phoenix",  # Diamondbacks' stadium
    # West Coast / Pacific Time
    "Anaheim Stadium": "America/Los_Angeles",
    "Edison International Field": "America/Los_Angeles",
    "Angel Stadium of Anaheim": "America/Los_Angeles",
    "Dodger Stadium": "America/Los_Angeles",
    "Oakland-Alameda County Coliseum": "America/Los_Angeles",
    "RingCentral Coliseum": "America/Los_Angeles",
    "PacBell Park": "America/Los_Angeles",
    "SBC Park": "America/Los_Angeles",
    "AT&T Park": "America/Los_Angeles",
    "Oracle Park": "America/Los_Angeles",
    "PETCO Park": "America/Los_Angeles",
    "Safeco Field": "America/Los_Angeles",
    "T-Mobile Park": "America/Los_Angeles",
    "Oakland Coliseum": "America/Los_Angeles",
    "O.co Coliseum": "America/Los_Angeles",
    "Petco Park": "America/Los_Angeles",
    "McAfee Coliseum": "America/Los_Angeles",
    "Angel Stadium": "America/Los_Angeles",

    # Mountain / Arizona Time
    "Bank One Ballpark": "America/Phoenix",
    "Chase Field": "America/Phoenix",
    "Coors Field": "America/Denver",

    # Central Time
    "Busch Stadium": "America/Chicago",
    "Busch Stadium I": "America/Chicago",
    "Busch Stadium II": "America/Chicago",
    "Busch Stadium III": "America/Chicago",
    "Comiskey Park": "America/Chicago",
    "U.S. Cellular Field": "America/Chicago",
    "Guaranteed Rate Field": "America/Chicago",
    "Miller Park": "America/Chicago",
    "American Family Field": "America/Chicago",
    "Astrodome": "America/Chicago",
    "Enron Field": "America/Chicago",
    "Minute Maid Park": "America/Chicago",
    "Royals Stadium": "America/Chicago",
    "Kauffman Stadium": "America/Chicago",
    "Hubert H. Humphrey Metrodome": "America/Chicago",
    "Target Field": "America/Chicago",
    "The Ballpark at Arlington": "America/Chicago",
    "Ameriquest Field": "America/Chicago",
    "Rangers Ballpark": "America/Chicago",
    "Globe Life Park": "America/Chicago",
    "Globe Life Field": "America/Chicago",
    "Field of Dreams": "America/Chicago",
    "Raley Field": "America/Los_Angeles",
    "Rickwood Field": "America/Chicago",
    "Rangers Ballpark in Arlington": "America/Chicago",
    "Globe Life Park in Arlington": "America/Chicago",

    # Eastern Time
    "Fenway Park": "America/New_York",
    "Yankee Stadium": "America/New_York",
    "Yankee Stadium I": "America/New_York",
    "Shea Stadium": "America/New_York",
    "Citi Field": "America/New_York",
    "Veterans Stadium": "America/New_York",
    "Citizens Bank Park": "America/New_York",
    "Three Rivers Stadium": "America/New_York",
    "PNC Park": "America/New_York",
    "Jacobs Field": "America/New_York",
    "Progressive Field": "America/New_York",
    "Great American Ball Park": "America/New_York",
    "Dunn Tire Park": "America/New_York",
    "Joe Robbie Stadium": "America/New_York",
    "Pro Player Park": "America/New_York",
    "Dolphin Stadium": "America/New_York",
    "Marlins Park": "America/New_York",
    "LoanDepot Park": "America/New_York",
    "SunTrust Park": "America/New_York",
    "Truist Park": "America/New_York",
    "Turner Field": "America/New_York",
    "Nationals Park": "America/New_York",
    "D.C. Stadium": "America/New_York",
    "Oriole Park at Camden Yards": "America/New_York",
    "TD Ballpark": "America/New_York",
    "George M. Steinbrenner Field": "America/New_York",
    "Fort Bragg Field": "America/New_York",
    "The Stadium at the ESPN Wide World of Sports": "America/New_York",
    "Journey Bank Ballpark": "America/New_York",
    "Sun Life Stadium": "America/New_York",
    "Hard Rock Stadium": "America/New_York",
    "Land Shark Park": "America/New_York",
    "Land Shark Stadium": "America/New_York",
    "Robert F. Kennedy Memorial Stadium": "America/New_York",
    "loanDepot park": "America/New_York",
    "Champion Stadium": "America/New_York",
    "Sahlen Field": "America/New_York",
    "The Ballpark at Disney's Wide World of Sports": "America/New_York",
    "Muncy Bank Ballpark": "America/New_York",
    "BB&T Ballpark": "America/New_York",

    # Canada
    "SkyDome": "America/Toronto",
    "Rogers Centre": "America/Toronto",

    # International / Special Events
    "Hiram Bithorn Stadium": "America/Puerto_Rico",
    "Estadio Alfredo Harp Helu": "America/Mexico_City",
    "Estadio de Beisbol Monterrey": "America/Monterrey",
    "London Stadium": "Europe/London",
    "Tokyo Dome": "Asia/Tokyo",
    "Gocheok Sky Dome": "Asia/Seoul",
    "Sydney Cricket Ground": "Australia/Sydney",
    "TD Ameritrade Park": "America/Chicago",

}
dates = ['2019-05-19', '2019-06-29', '2019-06-30', '2020-08-09', '2021-07-21', '2023-04-30', '2023-06-24', '2023-06-25', '2024-03-20', '2024-03-21', '2024-06-08', '2024-06-09']
date_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates]

date_pattern = re.compile(r'^[A-Za-z]+ \d{1,2}, \d{4}$')


def get_single():
    urls = []
    url_dic = {}
    conn = conn_pool.getconn()  # Get a connection from the pool
    with conn.cursor() as cur:
        cur.execute("""SELECT abr.external_abbr, g.date, v.venue_name, g.game_id
                        FROM games g
                        JOIN team_abbreviation_map abr ON g.venue_team_id = abr.team_id
                        JOIN venues v ON v.venue_id = g.venue_id
                        WHERE g.season_year BETWEEN 2019 AND 2024
                        AND g.level_id = 1 
                        AND g.type_id != 1  
                        AND g.date = ANY(%s)
                        AND g.game_id = 565067
                        AND (
                            SELECT COUNT(*)
                            FROM games g2
                            WHERE g2.venue_id = g.venue_id
                            AND g2.date = g.date
                            AND g2.level_id = 1
                        ) = 1""", (date_objs,))
        results = cur.fetchall()
        if results:
            for result in results:
                abr, date, venue_name, game_id = result
                if abr == "FLO":
                    abr = "MIA"
                date2 =  str(date).replace("-", "")
                url_end = f"{abr}/{abr}{date2}0"
                gen_url = f'https://www.baseball-reference.com/boxes/{url_end}.shtml'
                url_dic[gen_url] = (game_id, date, venue_name)
                urls.append(gen_url)
    
        for url, start_time in get_start_times(urls):
            game_id, date, venue_name = url_dic[url]
            if start_time:
                print(f"Game ID: {game_id}, Date: {date}, Venue: {venue_name}, Start Time: {start_time}")
                start_time = extract_time(start_time)
                time_zone = VENUE_TZ_MAP.get(venue_name)
                if time_zone:
                    local_tz = pytz.timezone(time_zone)
                    dt_str = f"{date} {start_time}"  # e.g. "2021-05-31 3:10 pm"
                    local_time = datetime.strptime(dt_str, "%Y-%m-%d %I:%M %p")

                    # Localize and convert to UTC
                    local_tz = pytz.timezone(time_zone)
                    localized = local_tz.localize(local_time)
                    utc_dt = localized.astimezone(pytz.utc)
                    dt = utc_dt.replace(tzinfo=None)  
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE games
                            SET datetime = %s
                            WHERE game_id = %s
                        """, (dt, game_id))
                    conn.commit()
                    print(f"Updated game {game_id} with start time {dt} (UTC)")
    
    conn_pool.putconn(conn)  # Return the connection to the pool
    print(f"Updated {len(urls)} games with start times.")

def get_double():
    urls = []
    url_dic = {}
    conn = conn_pool.getconn()  # Get a connection from the pool
    with conn.cursor() as cur:
        cur.execute("SELECT abr.external_abbr, g1.date, v.venue_name, g1.game_id, g2.game_id FROM games g1 JOIN games g2 ON (g1.venue_id = g2.venue_id and g1.date = g2.date and g1.game_id != g2.game_id) JOIN team_abbreviation_map abr ON g1.venue_team_id = abr.team_id JOIN venues v ON v.venue_id = g1.venue_id WHERE g1.season_year > 2018 AND g1.season_year < 2025 AND g1.level_id = 1 AND g1.type_id != 1 AND g2.level_id = 1 AND g2.type_id != 1 AND g1.game_time < g2.game_time")
        results = cur.fetchall()
        if results:
            for result in results:
                abr, date, venue_name, g1_id, g2_id = result
                if abr == "FLO":
                    abr = "MIA"
                date2 =  str(date).replace("-", "")
                url_end = f"{abr}/{abr}{date2}1"
                gen_url = f'https://www.baseball-reference.com/boxes/{url_end}.shtml'
                url_dic[gen_url] = (g1_id, date, venue_name)
                url_end2 = f"{abr}/{abr}{date2}2"
                gen_url2 = f'https://www.baseball-reference.com/boxes/{url_end2}.shtml'
                url_dic[gen_url2] = (g2_id, date, venue_name)
                urls.append(gen_url)
                urls.append(gen_url2)
    
    for url, start_time in get_start_times(urls):
        game_id, date, venue_name = url_dic[url]
        if start_time:
            print(f"Game ID: {game_id}, Date: {date}, Venue: {venue_name}, Start Time: {start_time}")
            start_time = extract_time(start_time)
            time_zone = VENUE_TZ_MAP.get(venue_name)
            if time_zone:
                local_tz = pytz.timezone(time_zone)
                dt_str = f"{date} {start_time}"  # e.g. "2021-05-31 3:10 pm"
                local_time = datetime.strptime(dt_str, "%Y-%m-%d %I:%M %p")

                # Localize and convert to UTC
                local_tz = pytz.timezone(time_zone)
                localized = local_tz.localize(local_time)
                utc_dt = localized.astimezone(pytz.utc)
                dt = utc_dt.replace(tzinfo=None)  
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE games
                        SET datetime = %s
                        WHERE game_id = %s
                    """, (dt, game_id))
                conn.commit()
                print(f"Updated game {game_id} with start time {dt} (UTC)")
    
    conn_pool.putconn(conn)  # Return the connection to the pool
    print(f"Updated {len(urls)} games with start times.")

            


def get_correct_time(game_id):
    
    # Fetch the game data from the MLB API
    game_id_str = str(game_id).zfill(6)
    conn = conn_pool.getconn()  # Get a connection from the pool
    try:
        request = requests.get(f"https://statsapi.mlb.com/api/v1/game/{game_id_str}/boxscore")
        request.raise_for_status()
        data = request.json()
    except Exception as e:
        print(f"Failed to fetch game {game_id_str}: {e}")
    
    if data:
        # Parse the time string
        try:
            info = data.get("info")
            for item in info:
                label = item.get("label", "").strip()
                
                if label == "First pitch":
                    time_str = item.get("value", "").strip().rstrip('.')
                
                elif label == "Venue":
                    venue = item.get("value", "").strip().rstrip('.')
                
                elif date_pattern.match(label):
                    date_str = label  # This is the actual date
            
            
            # Parse the ISO format string - already in UTC

            if date_str and time_str:
                # Example: "May 14, 2005 1:31 PM"
                full_datetime_str = f"{date_str} {time_str}"
                try:
                    naive_dt = datetime.strptime(full_datetime_str, "%B %d, %Y %I:%M %p")
                except ValueError as e:
                    print("Failed to parse datetime:", e)
                
                tz_name = VENUE_TZ_MAP.get(venue)

                if tz_name:
                    local_tz = pytz.timezone(tz_name)
                    localized_dt = local_tz.localize(naive_dt)
                    utc_dt = localized_dt.astimezone(pytz.utc)
                else:
                    print(f"[WARNING] Timezone not found for venue: {venue}", file=sys.stderr)
                # date_str = utc_dt.strftime("%Y-%m-%d")
                # time_str = utc_dt.strftime("%H:%M:%S")
                print(f'Date: {date_str}, Time: {time_str}, UTC DateTime: {utc_dt}')
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE games
                        SET date = %s,
                            game_time = %s
                        WHERE game_id = %s
                    """, (date_str, time_str, game_id))
            
            
            conn.commit()
            print(f"Updated game {game_id} with date {date_str} and time {time_str}")
        except Exception as e:
            print(f"Error processing game {game_id}: {e}")
    conn_pool.putconn(conn)  # Return the connection to the pool



def get_game_ids():
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT game_id FROM games WHERE season_year > 2018 AND season_year < 2024 AND type_id != 1 AND level_id = 1")
        game_ids = cursor.fetchall()
        return [game[0] for game in game_ids]
    except Exception as e:
        print(f"Error fetching game IDs: {e}")
        return []
    finally:
        cursor.close()
        conn_pool.putconn(conn)  # Return the connection to the pool


# game_ids = get_game_ids()

# with ThreadPoolExecutor(max_workers=5) as executor:
#     futures = []
#     for game_id in game_ids:
#         futures.append(executor.submit(get_correct_time, game_id))
#     for future in as_completed(futures):
#         try:
#             future.result()  # This will raise an exception if the function raised one
#         except Exception as e:
#             print(f"Error in thread: {e}")


#get_double()
get_single()

