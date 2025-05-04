import psycopg2
from passwords import password

conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=password, host="127.0.0.1", port="5432")
BATCH_SIZE = 10000
conn2 = psycopg2.connect(dbname="mlb_data", user="postgres", password=password, host="127.0.0.1", port="5432")

with conn.cursor(name='server-cursor') as cur:
    cur.itersize = BATCH_SIZE
    cur.execute("SELECT game_id, inning_num, inning_half, at_bat_num FROM at_bats WHERE inning_num > 1 and at_bat_num > 10000")

    batch = []
    for row in cur:
        game_id, inning_num, inning_half, at_bat_num = row
        batch.append((at_bat_num - 10001, game_id, inning_num, inning_half, at_bat_num))

        if len(batch) >= BATCH_SIZE:
            with conn2.cursor() as update_cur:
                update_cur.executemany("""
                    UPDATE at_bats
                    SET at_bat_num = %s
                    WHERE game_id = %s AND inning_num = %s AND inning_half = %s AND at_bat_num = %s
                """, batch)
            conn2.commit()
            print(f"Updated {len(batch)} records.")
            batch.clear()
            

    # Final batch
    if batch:
        with conn2.cursor() as update_cur:
            update_cur.executemany("""
                UPDATE at_bats
                SET at_bat_num = %s
                WHERE game_id = %s AND inning_num = %s AND inning_half = %s AND at_bat_num = %s
            """, batch)
        conn2.commit()
conn2.close()
conn.close()
print("All records updated successfully.")