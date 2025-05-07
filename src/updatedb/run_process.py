import subprocess
import time
import psycopg2
from datetime import datetime
import passwords

def check_remaining_games():
    try:
        conn = psycopg2.connect(
            dbname="mlb_data",
            user="postgres",
            password=passwords.password,
            host="localhost",
            port="5432"
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM game_info")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"Database error: {e}")
        return 0

def main():
    while True:
        remaining_games = check_remaining_games()
        
        if remaining_games == 0:
            print("No more games to process. Exiting...")
            break
        
        print(f"{datetime.now()} - Running process_data.py ({remaining_games} games remaining)")
        
        try:
            # Run the process_data.py script
            # subprocess.run(['../../venv/scripts/python', 'process_data.py'], check=True)
            subprocess.run(['../../venv/scripts/python', 'get_play_ids.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running process_data.py: {e}")
        
        # Wait 5 seconds before next iteration
        time.sleep(5)

if __name__ == "__main__":
    main()