import requests
from psycopg2 import pool
import passwords
from datetime import datetime, timedelta




conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")



base_url = "http://api.weatherbit.io/v2.0/history/subhourly?lat={lat}&lon={lon}&start_date={start_date}&end_date={end_date}&key={api_key}"


def get_weather(game_id):
    conn = conn_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT pitch_time
            FROM pitches
            WHERE game_id = %s
            ORDER BY pitch_time ASC
        """, (game_id,))
        row = cur.fetchone()
        if not row:
            print(f"[WARNING] No pitches found for game ID {game_id}")
            return None
        cur.execute("""
            SELECT v.latitude, v.longitude
            FROM venues v 
            JOIN games g ON v.venue_id = g.venue_id
            WHERE g.game_id = %s
        """, (game_id,))
        lat, lon = cur.fetchone()
        start_date = row[0].date()
        end_date = start_date + timedelta(days=5)
    conn_pool.putconn(conn)



def get_game_ids():
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT game_id FROM games WHERE season_year > 2014 AND type_id != 1 AND level_id = 1")
        game_ids = cursor.fetchall()
        return [game[0] for game in game_ids]
    except Exception as e:
        print(f"Error fetching game IDs: {e}")
        return []
    finally:
        cursor.close()
        conn_pool.putconn(conn)  # Return the connection to the pool





game_ids = get_game_ids()