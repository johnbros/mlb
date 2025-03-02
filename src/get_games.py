import psycopg2
import time
import requests
import json
from datetime import datetime, timedelta
import concurrent.futures
import passwords
import threading

failed_game_ids = set()
failed_game_ids_lock = threading.Lock()

def try_failed_game_ids():
    conn = psycopg2.connect(
        dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432"
    )
    cursor = conn.cursor()
    while failed_game_ids:
        with failed_game_ids_lock:
            for game_id in list(failed_game_ids):  # Use list to allow removal while iterating
                game_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_id}"
                game_response = requests.get(game_url)
                if game_response.status_code == 200:
                    game_data = game_response.json()
                    game_json = json.dumps(game_data)
                    print(date)
                    cursor.execute("""
                        INSERT INTO game_info (game_id, game_date, game_data)
                        VALUES (%s, %s, %s::jsonb)
                    """, (game_id, date, game_json))
                    conn.commit()
                    failed_game_ids.remove(game_id)
                else:
                    print(f"Failed retry for id {game_id}, code: {game_response.status_code}")

    cursor.close()
    conn.close()

def get_game_ids(start_date, end_date):
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={start_date}&endDate={end_date}"
    response = requests.get(url)
    conn = psycopg2.connect(
        dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432"
    )
    cursor = conn.cursor()
    
    if response.status_code == 200:
        data = response.json()
        for dates in data['dates']:
            for game in dates['games']:
                if game.get('seriesDescription') == "Regular Season":
                    game_id = str(game['gamePk']).zfill(6)
                    print(game_id)
                    date = dates['date']
                    cursor.execute("SELECT COUNT(*) FROM game_info WHERE game_id = %s", (game_id,))

                    result = cursor.fetchone()
                    if result[0] == 0:
                        game_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_id}"
                        game_response = requests.get(game_url)
                        if game_response.status_code == 200:
                            game_data = game_response.json()
                            game_json = json.dumps(game_data)
                            print(date)
                            cursor.execute("""
                                INSERT INTO game_info (game_id, game_date, game_data)
                                VALUES (%s, %s, %s::jsonb)
                            """, (game_id, date, game_json))
                            conn.commit()
                        else:
                            with failed_game_ids_lock:
                                failed_game_ids.add(game_id) 
                            print(f"Failed to fetch game data for game ID {game_id}, code: {game_response.status_code}")
    else:
        print(f"Failed to fetch game IDs for {start_date} to {end_date}")
    # Close DB connection
    cursor.close()
    conn.close()


def get_all_game_ids(start_year, end_year, start_month=3, end_month=10):
    current_year = start_year
    current_month = start_month
    
    while current_year <= end_year:
        start_date = datetime(current_year, current_month, 1)
        
        while start_date.month <= end_month:
            # Calculate the end of the current week (7 days from start_date)
            end_date = start_date + timedelta(days=6)
            
            # Get game IDs for this week
            get_game_ids(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            time.sleep(1/20)
            # Move to the next week
            start_date += timedelta(weeks=1)
        
        # After finishing the current year, move to the next year and reset month to March
        current_year += 1
        current_month = start_month



def split_work(start_year=1995, end_year=2024, num_threads=30):
    year_ranges = [(start_year + i, start_year + i) for i in range(num_threads)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(get_all_game_ids, start, end) for start, end in year_ranges]
        
        # Wait for all threads to finish
        for future in concurrent.futures.as_completed(futures):
            future.result()  # This raises exceptions if any thread fails


split_work()
try_failed_game_ids()

