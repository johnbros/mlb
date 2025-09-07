import passwords
from psycopg2 import pool
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")






def update_score_and_outs(game_id):
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            home_score = away_score = outs = None
            cur.execute("""
                SELECT inning_num, inning_half, at_bat_num, pitch_num, outs, home_score, away_score
                FROM pitches
                WHERE game_id = %s
                ORDER BY inning_num, inning_half, at_bat_num, pitch_num
            """, (game_id,))
            pitches = cur.fetchall()

            if not pitches:
                print(f"[ERROR] No pitches found for game ID {game_id}", file=sys.stderr)
                return
            #pitches = sorted(pitches, reverse=True)
            for inning_num, inning_half, at_bat_num, pitch_num, o, h_score, a_score in pitches:
                if h_score is not None:
                    home_score = h_score
                if a_score is not None:
                    away_score = a_score
                if o is not None:
                    outs = o
                
                cur.execute("""
                    UPDATE pitches
                    SET home_score = %s, away_score = %s, outs = %s
                    WHERE game_id = %s AND inning_num = %s AND inning_half = %s
                        AND at_bat_num = %s AND pitch_num = %s
                """, (home_score, away_score, outs, game_id, inning_num, inning_half, at_bat_num, pitch_num))


            conn.commit()
            print(f"[OK] Filled missing scores and outs for game ID {game_id}")

    except Exception as e:
        print(f"[ERROR] Game ID {game_id}: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        conn_pool.putconn(conn)



def update_base_states(game_id):
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            first = second = third = None

            cur.execute("""
                SELECT inning_num, inning_half, at_bat_num, pitch_num
                FROM pitches
                WHERE game_id = %s
                ORDER BY inning_num, inning_half, at_bat_num, pitch_num
            """, (game_id,))
            pitches = cur.fetchall()

            if not pitches:
                print(f"[WARNING] No pitches found for game ID {game_id}")
                return
            pitches = sorted(pitches, reverse=True)
            for inning_num, inning_half, at_bat_num, pitch_num in pitches:
                cur.execute("""
                    SELECT first, second, third
                    FROM base_states
                    WHERE game_id = %s AND inning_num = %s AND inning_half = %s
                      AND at_bat_num = %s AND pitch_num = %s
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num))
                
                row = cur.fetchone()
                if row:
                    first, second, third = row
                else:
                    print(f"[INSERT] Filling base state at {inning_num} {inning_half} {at_bat_num} {pitch_num} {first} {second} {third}")
                    try:
                        cur.execute("""
                            INSERT INTO base_states (
                                game_id, inning_num, inning_half, at_bat_num, pitch_num,
                                first, second, third
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (game_id, inning_num, inning_half, at_bat_num, pitch_num)
                                    DO NOTHING
                        """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, first, second, third))
                        print(f"[INSERTED] Rows affected: {cur.rowcount}")
                        conn.commit()
                    except Exception as e:
                        print(f"[ERROR] Game ID {game_id}: {e}", file=sys.stderr)
                    
            print(f"[OK] Filled missing base states for game ID {game_id}")

    except Exception as e:
        print(f"[ERROR] Game ID {game_id}: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        conn_pool.putconn(conn)


def update_defensive_states(game_id):
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            
            h_pos_1 = h_pos_2 = h_pos_3 = h_pos_4 = h_pos_5 = h_pos_6 = h_pos_7 = h_pos_8 = h_pos_9 = None
            a_pos_1 = a_pos_2 = a_pos_3 = a_pos_4 = a_pos_5 = a_pos_6 = a_pos_7 = a_pos_8 = a_pos_9 = None
            cur.execute("""
                SELECT pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home
                FROM defense_states
                WHERE game_id = %s and inning_num = 1 and inning_half = 'T' and at_bat_num = 1 and pitch_num = 0""", (game_id,))
            rows = cur.fetchall()
            if rows:
                for row in rows:

                    if row[9]:
                        h_pos_1, h_pos_2, h_pos_3, h_pos_4, h_pos_5, h_pos_6, h_pos_7, h_pos_8, h_pos_9 = row[:9]
                    else:
                        a_pos_1, a_pos_2, a_pos_3, a_pos_4, a_pos_5, a_pos_6, a_pos_7, a_pos_8, a_pos_9 = row[:9]
            cur.execute("""
                SELECT inning_num, inning_half, at_bat_num, pitch_num
                FROM pitches
                WHERE game_id = %s
                ORDER BY inning_num, inning_half, at_bat_num, pitch_num
            """, (game_id,))
            pitches = cur.fetchall()

            if not pitches:
                print(f"[WARNING] No pitches found for game ID {game_id}")
                return

            for inning_num, inning_half, at_bat_num, pitch_num in pitches:
                cur.execute("""
                    SELECT pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home
                    FROM defense_states
                    WHERE game_id = %s AND inning_num = %s AND inning_half = %s
                      AND at_bat_num = %s AND pitch_num = %s
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num))
                
                row = cur.fetchone()
                if row:
                    pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home = row
                    if home:
                        h_pos_1, h_pos_2, h_pos_3, h_pos_4, h_pos_5, h_pos_6, h_pos_7, h_pos_8, h_pos_9 = pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9
                    else:
                        a_pos_1, a_pos_2, a_pos_3, a_pos_4, a_pos_5, a_pos_6, a_pos_7, a_pos_8, a_pos_9 = pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9


                cur.execute("""
                    INSERT INTO defense_states (
                        game_id, inning_num, inning_half, at_bat_num, pitch_num,
                        pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, h_pos_1, h_pos_2, h_pos_3, h_pos_4, h_pos_5, h_pos_6, h_pos_7, h_pos_8, h_pos_9, True))
                cur.execute("""
                    INSERT INTO defense_states (
                        game_id, inning_num, inning_half, at_bat_num, pitch_num,
                        pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, a_pos_1, a_pos_2, a_pos_3, a_pos_4, a_pos_5, a_pos_6, a_pos_7, a_pos_8, a_pos_9, False))

            conn.commit()
            print(f"[OK] Filled missing defense states for game ID {game_id}")

    except Exception as e:
        print(f"[ERROR] Game ID {game_id}: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        conn_pool.putconn(conn)

def update_lineups(game_id):
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            a_first = a_second = a_third = a_fourth = a_fifth = a_sixth = a_seventh = a_eighth = a_ninth = None
            h_first = h_second = h_third = h_fourth = h_fifth = h_sixth = h_seventh = h_eighth = h_ninth = None
            cur.execute("""
                SELECT first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home
                FROM batting_lineups
                WHERE game_id = %s and inning_num = 1 and inning_half = 'T' and at_bat_num = 1 and pitch_num = 0""", (game_id,))
            rows = cur.fetchall()
            if rows:
                for row in rows:
                    if row[9]:
                        h_first, h_second, h_third, h_fourth, h_fifth, h_sixth, h_seventh, h_eighth, h_ninth = row[:9]
                    else:
                        a_first, a_second, a_third, a_fourth, a_fifth, a_sixth, a_seventh, a_eighth, a_ninth = row[:9]
            cur.execute("""
                SELECT inning_num, inning_half, at_bat_num, pitch_num
                FROM pitches
                WHERE game_id = %s
                ORDER BY inning_num, inning_half, at_bat_num, pitch_num
            """, (game_id,))
            pitches = cur.fetchall()

            if not pitches:
                print(f"[WARNING] No pitches found for game ID {game_id}")
                return

            for inning_num, inning_half, at_bat_num, pitch_num in pitches:
                cur.execute("""
                    SELECT first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home 
                    FROM batting_lineups
                    WHERE game_id = %s AND inning_num = %s AND inning_half = %s
                      AND at_bat_num = %s AND pitch_num = %s
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num))
                rows = cur.fetchall()
                if rows:
                    for row in rows:
                        first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home = row
                        if home:
                            h_first, h_second, h_third, h_fourth, h_fifth, h_sixth, h_seventh, h_eighth, h_ninth = first, second, third, fourth, fifth, sixth, seventh, eighth, ninth
                        else:
                            a_first, a_second, a_third, a_fourth, a_fifth, a_sixth, a_seventh, a_eighth, a_ninth = first, second, third, fourth, fifth, sixth, seventh, eighth, ninth
                
                cur.execute("""
                    INSERT INTO batting_lineups (
                        game_id, inning_num, inning_half, at_bat_num, pitch_num,
                        first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, h_first, h_second, h_third, h_fourth, h_fifth, h_sixth, h_seventh, h_eighth, h_ninth, True))
                cur.execute("""
                    INSERT INTO batting_lineups (
                        game_id, inning_num, inning_half, at_bat_num, pitch_num,
                        first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (game_id, inning_num, inning_half, at_bat_num, pitch_num, a_first, a_second, a_third, a_fourth, a_fifth, a_sixth, a_seventh, a_eighth, a_ninth, False))
            conn.commit()
            print(f"[OK] Filled missing defense states for game ID {game_id}")

    except Exception as e:
        print(f"[ERROR] Game ID {game_id}: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        conn_pool.putconn(conn)





def get_game_ids():
    conn = conn_pool.getconn()  # Get a connection from the pool
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT game_id FROM games WHERE season_year between 2015 and 20224 AND type_id != 1 AND level_id = 1")
        game_ids = cursor.fetchall()
        return [game[0] for game in game_ids]
    except Exception as e:
        print(f"Error fetching game IDs: {e}")
        return []
    finally:
        cursor.close()
        conn_pool.putconn(conn)  # Return the connection to the pool


def process_game(game_id):
    update_base_states(game_id)
    # update_defensive_states(game_id)
    # update_lineups(game_id)
    # update_score_and_outs(game_id)


game_ids = get_game_ids()
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = []
    for game_id in game_ids:
        futures.append(executor.submit(process_game, game_id))
    for future in as_completed(futures):
        try:
            future.result()  # This will raise an exception if the function raised one
        except Exception as e:
            print(f"Error in thread: {e}")
