import psycopg2
from psycopg2 import pool
from passwords import password
import requests
import json
from datetime import datetime
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{password}", host="127.0.0.1", port="5432")
failed_game_ids = set()

conn = conn_pool.getconn()  # Get a connection from the pool
with conn.cursor() as cur:
    cur.execute("""
        SELECT game_id from games where season_year > 2014 and level_id = 1 and type_id != 1""")
    rows = cur.fetchall()

conn_pool.putconn(conn)  # Return the connection to the pool
if rows:
    # Extract game IDs from the rows
    game_ids = [row[0] for row in rows]


def get_correct_time(game_ids):
    x = 0
    conn = conn_pool.getconn()
    for game_id in game_ids:
    # Fetch the game data from the MLB API
        game_id_str = str(game_id).zfill(6)
        data = None
        try:
            request = requests.get(f"https://statsapi.mlb.com/api/v1.1/game/{game_id_str}/feed/live/diffPatch")
            request.raise_for_status()
            data = request.json()
        except Exception as e:
            print(f"Failed to fetch game {game_id_str}: {e}")
            game_ids.remove(game_id)
            failed_game_ids.add(game_id_str)
            continue
        
        if data:
            # Parse the time string
            time_str = data['liveData']['plays']['allPlays'][0]['about']['startTime']
            
            # Parse the ISO format string - already in UTC
            dt = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
            
            # Format for database string fields
            date_str = dt.strftime("%Y-%m-%d")
            time_str = dt.strftime("%H:%M:%S")
            
            # Create a naive datetime for the timestamp field
            naive_dt = dt.replace(tzinfo=None)
            
            # print(f"Game ID: {game_id}")
            # print(f"UTC time: {dt}")
            # print(f"Naive datetime for DB: {naive_dt}")
            
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE games
                    SET date = %s,
                        game_time = %s,
                        datetime = %s
                    WHERE game_id = %s
                """, (date_str, time_str, naive_dt, game_id))
            
            x += 1
            print(x)
    conn.commit()
    conn_pool.putconn(conn)  # Return the connection to the pool



def split_into_chunks(array, chunk_size):
    """Split array into chunks of specified size"""
    return [array[i:i+chunk_size] for i in range(0, len(array), chunk_size)]



chunks = split_into_chunks(game_ids, 200)
with ThreadPoolExecutor(max_workers=16) as executor:
    futures = {executor.submit(get_correct_time, chunk): chunk for chunk in chunks}
    for future in as_completed(futures):
        chunk = futures[future]
        try:
            future.result()
        except Exception as e:
            print(f"Error processing chunk {chunk}: {e}")


# if failed_game_ids:
#     print(f"Retrying {len(failed_game_ids)} failed games...")
#     get_correct_time(failed_game_ids)


