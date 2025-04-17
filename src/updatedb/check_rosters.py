from psycopg2 import pool
import psycopg2
import concurrent.futures
import passwords
import requests
import threading
import queue
import time

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")


def get_team_roster(teamId, date):
    link = f'https://statsapi.mlb.com/api/v1/teams/{teamId}/roster?date={date}'
    response = requests.get(link)
    if response.status_code == 200:
        return response.json().get('roster', [])
    else:
        print(f"Error fetching roster for team {teamId} on {date}: {response.status_code}")
        return None
    

def check_roster(teamId, gameId, date):
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    # Query to get all team IDs and their corresponding game IDs and dates

    query2 = """
        DELETE FROM roster_games where team_id = %s and game_id = %s
    """
    query3 = """
        INSERT INTO roster_games (team_id, game_id, player_id) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
    """
    try:
        if date is None:
            cursor.close()
            conn_pool.putconn(conn)
            return
        date = date.strftime('%Y-%m-%d')
        roster = get_team_roster(teamId, date)
        cursor.execute(query2, (teamId, gameId))
        for player in roster:
            player_id = player.get('person', {}).get('id')
            cursor.execute(query3,(teamId, gameId, player_id))
            
        conn.commit()
    except Exception as e:
        print(f"Error checking roster for team {teamId} on {date}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn_pool.putconn(conn)






def pull_json(batch_size=500, out_queue=None):
    query1 = """
    SELECT tg.team_id, g.game_id, g.date FROM team_games tg left join
    games g on tg.game_id = g.game_id
    """
    # Establish the connection
    conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
    cursor = conn.cursor(name='server_cursor')  # Using server-side cursor to avoid memory issues

    # Execute the query to get the data from the game_info table
    cursor.execute(query1)

    # Fetch the data in batches and put each batch in the output queue for processing
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break  # Break when no more rows are available
        out_queue.put(batch)  # Put the batch in the queue
    # batch = cursor.fetchmany(batch_size)
    # out_queue.put(batch)  # Put the batch in the queue

    cursor.close()
    conn.close()
    out_queue.put(None)  # Sentinel value to indicate that no more batches are coming

def process_batches(out_queue, max_workers=95):
    while out_queue.empty():
        print("Waiting for data...")
        time.sleep(5)

    print("Got the data!")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        while True:
            team_games = out_queue.get()  # Get a batch from the queue
            if team_games is None:  # Sentinel value indicating end of data
                break
            # Process the batch concurrently
            futures = [executor.submit(check_roster, team_id, game_id, date) 
               for team_id, game_id, date in team_games]
            concurrent.futures.wait(futures) # Wait for all futures to finish
    print("All done!")

def main():
    out_queue = queue.Queue(maxsize=10)  # Create a queue to pass batches between threads

    # Create the thread for pulling data (fetching in batches)
    batching_thread = threading.Thread(target=pull_json, args=(250, out_queue))
    batching_thread.start()

    # Start the batch processing in the main thread or with a separate pool of threads
    process_batches(out_queue)

    # Wait for the batching thread to finish (though it's finished when no more batches are in the queue)
    batching_thread.join()


if __name__ == "__main__":
    main()