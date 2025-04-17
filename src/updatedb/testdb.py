import psycopg2
import json
import datetime
import passwords
import requests
# Database connection settings
DB_NAME = "mlb_data"
DB_USER = "postgres"  # Replace with your PostgreSQL username
DB_PASSWORD = f"{passwords.password}"  # Replace with your PostgreSQL password
DB_HOST = "127.0.0.1"  # or "localhost"
DB_PORT = "5432"  # Default PostgreSQL port
total = 0
try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

    # Create a cursor
    cur = conn.cursor()
    # Fetch the first row from the game_info table
    cur.execute(
    "SELECT game_data, game_type, league FROM game_info WHERE game_date = '1905-06-01'"
)
    row = cur.fetchone()  # Fetch the result
    data = row[0]
    game_type = row[1]
    league = row[2]
    home_stats = data.get('boxscore', {}).get('teams', {}).get('home', {}).get('players', {})
    print(home_stats)

    # Close connection
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Database error:", e)

# game_response = requests.get("https://baseballsavant.mlb.com/gf?game_pk=208913")
# game_data = game_response.json()
# with open('gamereq.json', 'w') as f:
#     json.dump(game_data, f, indent=4)

print(f"Total games: {total}")