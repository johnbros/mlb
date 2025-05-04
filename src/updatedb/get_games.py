from psycopg2 import pool
import time
import requests
import json
from datetime import datetime, timedelta
import concurrent.futures
import passwords
import threading
import queue


conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")

failed_game_ids = set()
failed_game_ids_lock = threading.Lock()
league_ids = {'MLB': 1, 'AAA': 11, 'AA': 12, 'A+': 13, 'A': 14}
#league_ids = {'MLB': 1, 'AAA': 11}
game_queue = queue.Queue()

def try_failed_game_ids():
    # conn = psycopg2.connect(
    #     dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432"
    # )
    # cursor = conn.cursor()
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        while not game_queue.empty():
            try:
                game_id, date, league, game_type = game_queue.get_nowait()
                cursor.execute("SELECT COUNT(*) FROM game_info WHERE game_id = %s", (game_id,))
                result = cursor.fetchone()
                if result[0] == 0:
                    game_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_id}"
                    game_response = requests.get(game_url)
                    if game_response.status_code == 200:
                        game_data = game_response.json()
                        if game_data.get("game_status") == "F" or game_data.get("game_status") == "FR":
                            game_json = json.dumps(game_data)
                            print(date)
                            cursor.execute("""
                                INSERT INTO game_info (game_id, game_date, league, game_type, game_data)
                                VALUES (%s, %s, %s, %s, %s::jsonb)
                            """, (game_id, date, league, game_type, game_json))
                            conn.commit()
                            game_queue.task_done()
                    else:
                        print(f"Failed retry for id {game_id}, code: {game_response.status_code}")
                else:
                    game_queue.task_done()
            except queue.Empty:
                pass
    finally:
        cursor.close()
        conn_pool.putconn(conn)
    # cursor.close()
    # conn.close()

def do_failed():
    for game in failed_game_ids:
        game_queue.put(game)
    with concurrent.futures.ThreadPoolExecutor(max_workers=95) as executor:
        executor.map(lambda _: try_failed_game_ids(), range(95))
    


def get_game_jsons(league, dates):
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        for game in dates['games']:
            game_type = game.get('seriesDescription')
            gamePk = game.get('gamePk')
            game_id = str(game.get('gamePk')).zfill(6)
            date = dates.get('date')

            if game_type in ["Regular Season", "Spring Training", "Wild Card Game", "Wild Card",
                             "Division Series", "League Championship Series", "World Series", "Championship"]:
                cursor.execute("SELECT COUNT(*) FROM games WHERE game_id = %s", (gamePk,))
                result = cursor.fetchone()
                
                if result[0] == 0:
                    game_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_id}"
                    game_response = requests.get(game_url)

                    if game_response.status_code == 200:
                        game_data = game_response.json()
                        status = game_data.get("game_status")
                        if status in {'F', 'FR', 'FG', 'FO'}:
                            game_json = json.dumps(game_data)
                            print(date)
                            cursor.execute("""
                                INSERT INTO game_info (game_id, game_date, league, game_type, game_data)
                                VALUES (%s, %s, %s, %s, %s::jsonb)
                            """, (game_id, date, league, game_type, game_json))
                            conn.commit()
                    else:
                        with failed_game_ids_lock:
                            failed_game_ids.add((game_id, date, league, game_type)) 
                        print(f"Failed to fetch game data for game ID {game_id}, code: {game_response.status_code}")
    
    finally:
        cursor.close()
        conn_pool.putconn(conn)


def get_game_ids(start_date, end_date, league, sport_id):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId={sport_id}&startDate={start_date}&endDate={end_date}"
    response = requests.get(url)
    

    if response.status_code == 200:
        data = response.json()
        with concurrent.futures.ThreadPoolExecutor(max_workers=19) as executor:
            results = executor.map(lambda date: get_game_jsons(league, date), data['dates'])
    else:
        print(f"Failed to fetch game IDs for {start_date} to {end_date}")
                
            


def get_all_game_ids(start_year = 1903, end_year = 2025, start_month=1, end_month=12):
    
    leagues = {
       'MLB': range(start_year, end_year + 1),
       'AAA': range(max(2005, start_year), end_year + 1),
       'AA': range(max(2005, start_year), end_year + 1),
       'A+': range(max(2005, start_year), end_year + 1),
       'A': range(max(2005, start_year), end_year + 1),
    }
    # leagues = {
    #     'MLB': range(start_year, end_year + 1),
    # }

    # 10 Threads for different years
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as year_executor:
        futures = []
        for league, years in leagues.items():
            for y in years:
                futures.append(year_executor.submit(get_game_ids, f'{y}-01-01', f'{y}-12-31', league, league_ids[league]))

        concurrent.futures.wait(futures)  # Ensures all tasks complete before exiting




# def split_work(start_year=1995, end_year=2024, num_threads=30):
#     year_ranges = [(start_year + i, start_year + i) for i in range(num_threads)]

#     with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
#         futures = [executor.submit(get_all_game_ids, start, end) for start, end in year_ranges]
        
#         # Wait for all threads to finish
#         for future in concurrent.futures.as_completed(futures):
#             future.result()  # This raises exceptions if any thread fails


get_all_game_ids(start_year=2015, end_year=2025)

do_failed()

conn_pool.closeall()

