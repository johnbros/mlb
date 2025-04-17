from psycopg2 import pool, IntegrityError
import psycopg2
import passwords
import requests
from endpoints import VENUES_ENDPOINT


    
#todo: Think of a way to approach this.
def validate_at_bats():
    game_date = '2015-01-01'
    query = """
    select g.game_id, ab.bip_id, p.pitch_outcome_id from games g
    left join at_bats ab on g.game_id = ab.game_id
    left join pitches p on g.game_id = p.game_id
    where g.game_date > %s"""
    



def validate_venues():
    query = """
    select g.venue_id from games g
    left join venues v on g.venue_id = v.venue_id
    where v.venue_id is null
    """
    insert_query = """
    insert into venues (venue_id, venue_name) values (%s, %s) on conflict (venue_id) do nothing
    """
    conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    if result:
        for venue in result:
            venue_info = requests.get(f"{VENUES_ENDPOINT}/{venue[0]}")
            if venue_info.status_code == 200:
                venue_data = venue_info.json()
                if venue_data:
                    v = venue_data.get('venues', [None])[0]
                    venue_id = v.get('id')
                    venue_name = v.get('name')
                    if venue_id and venue_name:
                        try:
                            cursor.execute(insert_query, (venue_id, venue_name))
                            conn.commit()
                        except IntegrityError:
                            conn.rollback()
                            print(f"Duplicate entry for venue_id: {venue_id}")
                        except Exception as e:
                            conn.rollback()
                            print(f"Error inserting venue_id {venue_id}: {e}")

    cursor.close()
    conn.close()


# validate_venues() this is done for now foreign key added all venues should be in the db now

                        


