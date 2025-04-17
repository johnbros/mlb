from multiprocessing import Pool
from psycopg2 import pool
import psycopg2
import passwords

conn_pool = pool.SimpleConnectionPool(5, 20, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
def backfill():
    conn = conn_pool.getconn()
    cursor = conn.cursor()

    for table in ["Player_Games_Pitching", "Player_Games_Hitting", "Player_Games_Fielding"]:
        query = f"""
        INSERT INTO roster_games (team_id, game_id, player_id)
        SELECT team_id, game_id, player_id
        FROM {table} pg
        WHERE NOT EXISTS (
        SELECT 1
        FROM roster_games r
        WHERE r.team_id = pg.team_id
        AND r.game_id = pg.game_id
        AND r.player_id = pg.player_id
        );
        """
        cursor.execute(query)
        conn.commit()

    cursor.close()
    conn_pool.putconn(conn)
    print("All done")

# Run in parallel
def main():
    # years = list(range(1903, 2024))  
    # for year in years:
    #     backfill_by_year(year)
    #     print(f"Backfilled roster_games for year {year}")
    backfill()


if __name__ == "__main__":
    main()
