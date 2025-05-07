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





def get_game_ids():
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        cursor.execute("select distinct g.game_id from pitches p join games g on p.game_id = g.game_id where season_year >= 2015 and type_id != 1 and level_id = 1 and p.play_id is null")
        game_ids = cursor.fetchall()
        return [game[0] for game in game_ids]
    except Exception as e:
        print(f"Error fetching game IDs: {e}")
        return []
    finally:
        cursor.close()
        conn_pool.putconn(conn)  # Return the connection to the pool


def get_game_jsons(game_id):    
    conn = conn_pool.getconn()
    game_id_str = str(game_id).zfill(6)
    game_url = f"https://baseballsavant.mlb.com/gf?game_pk={game_id_str}"
    game_response = requests.get(game_url)
    if game_response.status_code == 200:
        game_data = game_response.json()
        status = game_data.get("game_status")
        if status in {'F', 'FR', 'FG', 'FO'}:
            game_json = json.dumps(game_data)
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO game_info (game_id, game_date, league, game_type, game_data)
                    VALUES (%s, %s, %s, %s, %s::jsonb)
                """, (game_id, '2015-01-01', 'MLB', 2, game_json))
            conn.commit()
    else: 
        print(f"Failed to fetch game data for game ID {game_id}, code: {game_response.status_code}")





game_ids = get_game_ids()

for game_id in game_ids:
    get_game_jsons(game_id)