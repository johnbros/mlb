import psycopg2
from passwords import password
##########Populate Static Tables with Data##########
conn = psycopg2.connect(dbname="mlb_data", user="postgres", password=f"{password}", host="127.0.0.1", port="5432")
cursor = conn.cursor()

#Populate the Levels Table#
cursor.execute("""
    INSERT INTO levels (level_id, level_abbreviation)
    VALUES 
    (1, 'MLB'),
    (11, 'AAA'),
    (12, 'AA'),
    (13, 'A+'),
    (14, 'A')
""")

#Populate the Game Types Table#
cursor.execute("""
    INSERT INTO game_types (type_id, type_name)
    VALUES 
    (1, 'Spring Training'),
    (2, 'Regular Season'),
    (3, 'Wild Card Game'),
    (4, 'Championship'),
    (5, 'Division Series'),
    (6, 'League Championship Series'),
    (7, 'World Series')
""")

cursor.close()
conn.commit()
conn.close()