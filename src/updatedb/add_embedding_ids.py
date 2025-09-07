import psycopg2
from passwords import password

# mlb_data=# select * from positions;
#  position_id |    position_name
# -------------+---------------------
#           17 | Two-Way Player
#           18 | Utility
#           19 | Left-Handed Pitcher
#            4 | Second Base
#            8 | Outfielder
#            9 | Outfielder
#            6 | Shortstop
#            3 | First Base
#            7 | Outfielder
#            1 | Pitcher
#            2 | Catcher
#            5 | Third Base
#           12 | Outfield
#           11 | Pinch Hitter
#           -1 | Unknown
#           10 | Designated Hitter
#           13 | Infield
#           14 | Unknown
#           15 | Relief Pitcher
#           16 | Starting Pitcher
position_ids = [18,4,8,9,6,3,7,2,5,11,12,-1,10,13,14]

def add_embedding_ids_to_db():
    conn = psycopg2.connect(
        host="localhost",
        database="mlb_data",
        user = "postgres",
        password = password,
        port = 5432
    )
    pos_player_pitching_id = -1
    with conn.cursor() as cur:
        # Get the list of all tables in the database
        cur.execute("SELECT player_id, position_id from players order by player_id")
        rows = cur.fetchall()
        player_ids = []
        if rows:
            for idx, (player_id, position_id) in enumerate(rows):
                # Update the embedding_id for each player
                if position_id in position_ids:
                    pitching_id = pos_player_pitching_id
                else:
                    pitching_id = idx
                try:

                    cur.execute(
                        "UPDATE players SET embedding_id = %s, pitching_id = %s WHERE player_id = %s",
                        (idx, pitching_id, player_id)
                    )
                    conn.commit()
                    print(f"Updated player_id {player_id} with embedding_id {idx}")
                    print(f"Updated player_id {player_id} with pitching_id {pitching_id}")
                except Exception as e:
                    print(f"Error updating player_id {player_id}: {e}")
                    conn.rollback()

if __name__ == "__main__":
    add_embedding_ids_to_db()
    print("Embedding IDs added to the database.")
        