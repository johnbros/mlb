import passwords
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime, timedelta

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")



def round_to_nearest_quarter_utc(iso_str):
    # Parse ISO timestamp with timezone info
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))

    # Make it naive (strip timezone) — assuming already in UTC
    dt = dt.replace(tzinfo=None)

    # Round to nearest quarter hour
    minutes = dt.minute
    remainder = minutes % 15
    if remainder >= 7.5:
        dt += timedelta(minutes=(15 - remainder))
    else:
        dt -= timedelta(minutes=remainder)

    # Strip seconds/microseconds
    return dt.replace(second=0, microsecond=0)

def get_pitch_times(game_id):
    gamePk = str(game_id).zfill(6)
    req_url = f'https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live/diffPatch'
    response = requests.get(req_url)
    if response.status_code == 200:
        data = response.json()
        plays = data.get('liveData', {}).get('plays', {}).get('allPlays', [])
        if not plays:
            print(f"[WARNING] No plays found for game ID {game_id}")
            return None
        for play in plays:
            playEvents = play.get('playEvents', [])
            if not playEvents:
                print(f"[WARNING] No play events found for game ID {game_id}")
                return None
            for event in playEvents:
                isPitch = event.get('isPitch')
                if isPitch:
                    pitch_time = event.get('startTime')
                    pitch_time = round_to_nearest_quarter_utc(pitch_time)
                    play_id = event.get('playId')
                    conn = conn_pool.getconn()
                    try:
                        with conn.cursor() as cur:
                            cur.execute("""
                                SELECT * from pitches 
                                WHERE play_id = %s
                            """, (play_id,))
                            result = cur.fetchone()
                            if result:
                                cur.execute("""
                                    UPDATE pitches 
                                    SET pitch_time = %s 
                                    WHERE play_id = %s
                                """, (pitch_time, play_id))
                                conn.commit()
                            else:
                                print(f"[WARNING] No pitch found for play ID {play_id} in game ID {game_id}")
                    except Exception as e:
                        print(f"[ERROR] Game ID {game_id}, Play ID {play_id}: {e}")
                        conn.rollback()
                    finally:
                        conn_pool.putconn(conn)
                    print(f"[OK] Updated pitch time for play ID {play_id} in game ID {game_id}")
    else:
        print(f"Failed to fetch data for game ID {game_id}, status code: {response.status_code}")













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



if __name__ == "__main__":
    game_ids = get_game_ids()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(get_pitch_times, gid) for gid in game_ids]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"[ERROR] Exception during pitch time update: {e}")