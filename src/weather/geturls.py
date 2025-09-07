
import requests
from psycopg2 import pool
from passwords import password, weather_api_keys
from datetime import datetime, timedelta
from psycopg2.extras import Json, execute_batch
import time
from dateutil.relativedelta import relativedelta


conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{password}", host="127.0.0.1", port="5432")

links = "reqlinks.txt"

checkpoint_file = "weather_checkpoints.txt"
completed = set()
try:
    with open(checkpoint_file, "r") as f:
        for line in f:
            completed.add(line.strip())
except FileNotFoundError:
    pass  # first time running



base_url = "http://api.weatherbit.io/v2.0/history/subhourly?lat={lat}&lon={lon}&start_date={start_date}&end_date={end_date}"



years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]

def get_weather():
    conn = conn_pool.getconn()
    with conn.cursor() as cur:
        for year in years:
            cur.execute("""
                SELECT distinct v.latitude, v.longitude, v.venue_id
                FROM venues v 
                JOIN games g ON v.venue_id = g.venue_id
                WHERE g.season_year = %s and g.type_id != 1 and g.level_id = 1
            """, (year,))
            rows = cur.fetchall()
            
            try:
                for row in rows:
                    lat, lon, venue_id = row
                    cur.execute("""
                        SELECT MIN(datetime) as start_date, MAX(datetime) as end_date
                        FROM games
                        WHERE season_year = %s and type_id != 1 and level_id = 1 and venue_id = %s
                    """, (year, venue_id,))
                    dates = cur.fetchone()
                    start_date, end_date = dates
                    start_date = start_date.strftime("%Y-%m-%d")
                    end_date = end_date.strftime("%Y-%m-%d")
                    lat = round(lat, 5)
                    lon = round(lon, 5)
                    current_start = datetime.strptime(start_date, "%Y-%m-%d")
                    final_end = datetime.strptime(end_date, "%Y-%m-%d")
                    while current_start < final_end:
                        current_end = min(current_start + relativedelta(months=1), final_end)
                        start_str = current_start.strftime("%Y-%m-%d")
                        end_str = current_end.strftime("%Y-%m-%d")
                        key = f"{lat},{lon},{start_str}"
                        if key in completed:
                            print(f"[SKIP] Already fetched for {key}")
                            current_start = current_end
                            continue

                        req_url = base_url.format(lat=lat, lon=lon, start_date=start_str, end_date=end_str)
                        with open(links, 'a') as f:
                            f.write(req_url + "\n")
                        current_start = current_end

            except Exception as e:
                print(f"Unexpected error: {e}")
                return None
    conn_pool.putconn(conn)  # Return the connection to the pool
    return None
            

get_weather()