import requests
from psycopg2 import pool
from passwords import password, weather_api_keys
from datetime import datetime, timedelta
from psycopg2.extras import Json, execute_batch
import time
from dateutil.relativedelta import relativedelta

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{password}", host="127.0.0.1", port="5432")


base_url = "http://api.weatherbit.io/v2.0/history/subhourly?lat={lat}&lon={lon}&start_date={start_date}&end_date={end_date}&key={weather_api_key}"

checkpoint_file = "weather_checkpoints.txt"
completed = set()
try:
    with open(checkpoint_file, "r") as f:
        for line in f:
            completed.add(line.strip())
except FileNotFoundError:
    pass  # first time running


def get_weather():
    conn = conn_pool.getconn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT distinct v.latitude, v.longitude
            FROM venues v 
            JOIN games g ON v.venue_id = g.venue_id
            WHERE g.season_year = 2015 and g.type_id != 1 and g.level_id = 1
        """)
        rows = cur.fetchall()
        cur.execute("""
                    SELECT MIN(datetime) as start_date, MAX(datetime) as end_date
                    FROM games
                    WHERE season_year = 2015 and type_id != 1 and level_id = 1
                """)
        dates = cur.fetchone()
        start_date, end_date = dates
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        try:
            for row in rows:
                lat, lon = row
                lat = round(lat, 5)
                lon = round(lon, 5)
                current_start = datetime.strptime(start_date, "%Y-%m-%d")
                final_end = datetime.strptime(end_date, "%Y-%m-%d")
                insert = []
                new_keys = set()
                while current_start < final_end:
                    current_end = min(current_start + relativedelta(months=1), final_end)
                    start_str = current_start.strftime("%Y-%m-%d")
                    end_str = current_end.strftime("%Y-%m-%d")
                    key = f"{lat},{lon},{start_str}"
                    if key in completed:
                        print(f"[SKIP] Already fetched for {key}")
                        current_start = current_end
                        continue

                    req_url = base_url.format(lat=lat, lon=lon, start_date=start_str, end_date=end_str, weather_api_key=weather_api_keys)
                    
                    try:
                        response = requests.get(req_url)
                        if response.status_code == 429:
                            print(f"[WARNING] Request limit exceeded for {weather_api_keys}.")
                            print("[DETAILS] Response headers:", response.headers)
                            print("[DETAILS] Response text:", response.text)
                            try:
                                print(f"[DETAILS] {response.json()}")
                            except Exception:
                                print(f"[DETAILS] {response.text}")
                            continue
                        response.raise_for_status()
                        r_json = response.json()
                        data = r_json.get('data', [])
                        if not data:
                            print(f"[WARNING] No weather data found for coordinates: {lat}, {lon}")
                            continue
                        
                        for weather in data:
                            timestamp = weather.get('timestamp_utc')
                            if not timestamp:
                                print(f"[WARNING] No timestamp: {weather} for coordinates: {lat}, {lon}")
                                continue
                            weather_entry = (timestamp, lat, lon, weather)
                            insert.append(weather_entry)
                            new_keys.add(key)
                        
                    except requests.exceptions.RequestException as e:
                        print(f"[ERROR] Weather fetch failed for ({lat}, {lon}) from {start_str} to {end_str}: {e}")
                    current_start = current_end
                    time.sleep(5)
                
                if insert:
                    execute_batch(cur, """
                            INSERT INTO weather (timestamp_utc, latitude, longitude, weather_data)
                            VALUES (%s, %s, %s, %s::jsonb)
                            ON CONFLICT (timestamp_utc, latitude, longitude) DO NOTHING
                        """, [(ts, lat, lon, Json(w)) for ts, lat, lon, w in insert])
                    conn.commit()
                    print(f"[INFO] Inserted {len(insert)} rows for ({lat}, {lon}) from {start_date} to {end_date}")
                    with open(checkpoint_file, "a") as f:
                        for k in new_keys:
                            f.write(f"{k}\n")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    conn_pool.putconn(conn)





if __name__ == "__main__":
    get_weather()