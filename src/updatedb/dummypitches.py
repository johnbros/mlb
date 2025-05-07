import passwords
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")


def dummy_pitches(game_id):
    conn = conn_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("""
        SELECT DISTINCT inning_num, inning_half, at_bat_num, pitch_num
        FROM base_states
        WHERE game_id = %s
    """, (game_id,))
        base_points = set(cur.fetchall())

        cur.execute("""
            SELECT inning_num, inning_half, at_bat_num, pitch_num
            FROM pitches
            WHERE game_id = %s
        """, (game_id,))
        real_pitches = set(cur.fetchall())

        missing_pitches = base_points - real_pitches

        for (inning_num, inning_half, at_bat_num, pitch_num) in missing_pitches:
            cur.execute("""
                INSERT INTO pitches (
                    game_id, inning_num, inning_half, at_bat_num, pitch_num, dummy
                ) VALUES (%s, %s, %s, %s, %s, TRUE)
                ON CONFLICT DO NOTHING
            """, (game_id, inning_num, inning_half, at_bat_num, pitch_num))




def get_game_ids():
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT game_id FROM games WHERE season_year > 2004 AND season_year < 2025 AND type_id != 1 AND level_id = 1")
        game_ids = cursor.fetchall()
        return [game[0] for game in game_ids]
    except Exception as e:
        print(f"Error fetching game IDs: {e}")
        return []
    finally:
        cursor.close()
        conn_pool.putconn(conn)  # Return the connection to the pool



game_ids = get_game_ids()

with ThreadPoolExecutor(max_workers=45) as executor:
    futures = []
    for game_id in game_ids:
        futures.append(executor.submit(dummy_pitches, game_id))
    for future in as_completed(futures):
        try:
            future.result()  # This will raise an exception if the function raised one
        except Exception as e:
            print(f"Error in thread: {e}")