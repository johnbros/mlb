from psycopg2 import pool, sql
import passwords
import os
import sys
import threading
import concurrent.futures
import unicodedata

from concurrent.futures import ThreadPoolExecutor, as_completed
from GameStateTracker import GameStateTracker
from convert_time import convert_to_utc, TEAM_TIMEZONES

rootdir = "C:/Users/johnb/Desktop/Sports/mlb/src/Play-By-Play"
conn_pool = pool.SimpleConnectionPool(5, 95, dbname="mlb_data", user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")

aliases = {
    'Matt Joyce': 'Matthew Joyce',
    'Wilfredo Ledezma':'Wil Ledezma',
    'Hee-Seop Choi': 'Hee Seop Choi',
    'David Matranga': 'Dave Matranga',
    'CC Sabathia': 'C.C. Sabathia',
    'Juan Carlos Oviedo': 'Leo Nunez',
    'Michael Morse': 'Mike Morse',
    'Chad Orvella': 'Cha Orvella',
    'Pete Laforest': 'Pete LaForest',
    'Alex Rios': 'Alexis Rios',
    'Vinnie Chulk': 'Vinny Chulk',
    'Geremi Gonzalez': 'Jeremi Gonzalez',
    'Steve Schmoll': 'Stephen Schmoll',
    'D.J. Houlton': 'Dennis Houlton',
    'Michael Restovich': 'Mike Restovich',
    'JD Closser': 'J.D. Closser',
    'Abraham M. Nunez': 'Abraham Nunez',
    'Dan Kolb': 'Danny Kolb',
    'Chin Hui Tsao': 'Chin-Hui Tsao',
    'Kazuo Matsui': 'Kaz Matsui',
    'Bill Hall': 'Billy Hall',
    'Hung-Chih Kuo': 'Hong-Chih Kuo',
    'Chin-hui Tsao': 'Chin-Hui Tsao',
    'Rickie Weeks Jr.': 'Rickie Weeks',
    'Kaz Ishii': 'Kazuhisa Ishii',
    'Eric Young Jr.': 'Eric Young',
    'Dave McCarty': 'David McCarty',
    'Jeff Fiorentino': 'Jeffrey Fiorentino',
    'Russell Branyan': 'Russ Branyan',
    'Daniel Ortmeier': 'Dan Ortmeier',
    'Roman Colon': 'Roman Colon',
    'Santiago Casilla': 'Jairo Garcia',
    'Matt Cepicky': 'Matthew Cepicky',
    'Bob Keppel': 'Bobby Keppel',
    # add more known aliases as you encounter them
}


player_lookup = {}

def normalize_name(name):
    """
    Convert accented characters to plain ASCII characters.
    """
    normalized = unicodedata.normalize('NFD', name)
    return ''.join([char for char in normalized if unicodedata.category(char) != 'Mn'])


def load_player_lookup():
    lookup = {'Roman Colono': 430638,
                 'Joshn Beckett': 277417,
                 'Brian Moeller': 119215,
                 'Eric MIlton': 132980,
                 'Alex Rios': 425567,
                 'Kendry Morales': 434778,
                 'Yuniesky Betancourt-Pere': 435358,
                 'Andy Sisco': 434878,
                 'Bradley Hawpe': 425547,
                 'Danny Baez': 276056,
                 'Jae Seo': 150242,
                 'Fausto Carmona': 433584,
                 'Daniel Haren': 429717,
                 'Wily Pena': 276377,
                 'James Hoey': 460597,
                 'Edison Volquez': 450172,
                 'Jon Papelbon': 449097,
                 'Jeffrey DaVanon': 237800,
                 'Joseph Blanton': 430599,
                 'William Ohman': 349193,
                 'Peter Orr': 434681,
                 'Bill Hall': 407849,
                 'Manuel Corpas': 466918,
                 'Vinnie Chulk': 425562,
                 'Joseph Thurston': 407871,
                 'Nathan Robertson': 425146,
                 'Chad Orvella': 435378,
                 'Douglas Waechter': 425531,
                 'Lenny Dinardo': 430837,
                 'Santiago Casilla': 433586,
                 'Dan Kolb': 150285,
                 'Jorge DeLaRosa': 407822,
                 'Sunny Kim': 279615,
                 'Yurendell de Caster': 436913,
                 'R Andrew Brown': 408046,
                 'Patrick Misch': 435619,
                 'Philip Hughes': 461833,
                 'Rick VandenHurk': 462995,
                 'Eulogio de la Cruz': 448842,
                 'Alejandro de Aza': 457477,
                 'Jonathan Meloan': 459974,
                 'Joseph Bisenius': 445202,
                 'Byung-Hyun': 218294,
                 'Dave Davidson': 452656,
                 'Van Benschoten': 429724,
                 'Steven Pearce': 456665,
                 'Rocci Baldelli': 408306,
                 'Masahide Kobayashi': 493131,
                 'Garrett Anderson': 110236,
                 'Daniel Ortmeier': 430931,
                 'Kazuo Matsui': 430565,
                 'Sean Kazmar': 453527,
                 'Danny Herrera': 502609,
                 'Ron Belliard': 150071,
                 'Kameron Mickolio': 501874,
                 'Michael Ekstrom': 455091,
                 'Mike Hollimon': 446132,
                 'Sandy Alomar': 110184,
                 'Andrew Carpenter': 502165,
                 'Colin Balester': 444446,
                 'Kevin Frank001': 435623,
                 'Russell Branyan': 137140,
                 'Bob Keppel': 430670,
                 'Robert Mosebach': 458595,
                 'Jorge de la Rosa': 407822,
                 'Daniel Ray Herrera': 502609,
                 'Donald Veal': 453264,
                 'Samuel Gervacio': 463645,
                 'Lucas French': 448287,
                 'Christopher Leroux': 460092,
                 'Michael Dunn': 445197,
                 'Howard Kendrick': 435062,
                 'Ramon S. Ramirez': 430673,
                 'Steven Tolleson': 476270,
                 'Dane Eveland': 445968,
                 'Alejandro Sanabia': 502253,
                 'Russ Mitchell': 452199,
                 'Andrew Oliver': 501989,
                 'Jeffrey Marquez': 457812,
                 'Jeremy Mejia': 516769,
                 'Dee Gordon': 543829,
                 'J.B. Shuck': 543776,
                 'Zachary Britton': 502154,
                 'Henderson Alvarez': 506693,
                 'Nathan Adcock': 502264,
                 'Alexander Torres': 456776,
                 'Michael Fiers': 571666,
                 'Eulogio De La Cruz': 448842,
                 'Peter Kozma': 518902,
                 'Thomas Field': 474494,
                 'Josh Rodriguez': 446044,
                 'Tom Milone': 543548,
                 'Manuel Pina': 444489,
                 'Val Pascucci': 407831,
                 'Robbie Ross': 543726,
                 'A.J. Pollock': 572041,
                 'Tom Layne': 518927,
                 'A.J. Ramos': 573109,
                 'Stuart Pomeranz': 456421,
                 'Dane DeLaRosa': 451773,
                 'Chuckie Fick': 518674,
                 'Rick van den Hurk': 462995,
                 'Matt Joyce': 459964,
                 'Luis Antonio Jimenez': 455921,
                 'Michael Morse': 434604,
                 'Hyun-Jin Ryu': 547943,
                 'Eury de la Rosa': 545001,
                 'Jon Niese': 477003,
                 'Reymond Fuentes': 571681,
                 'Nathan Karns': 501992,
                 'Jon Lannan': 458709,
                 'Philip Gosselin': 594838,
                 'Matt den Dekker': 544925,
                 'J.C. Ramirez': 500724,
                 'Jose de la Torre': 473265,
                 'JR Murphy': 571974,
                 'Jackie Bradley': 598265,
                 'Rubby de la Rosa': 523989,
                 'Jose de la Torre': 473265,
                 'Zach Rosscup': 573127,
                 'Juan Nicasi0': 504379,
                 'Xavier Cedemo': 458584,
                 'Charlier Morton': 450203,
                 'J.R. Murphy': 571974,
                 'Juan Carlos Oviedo': 434663,
                 'Roman Ali Solis': 455378,
                 'Michael Foltynewicz': 592314,
                 'Anthony Gwynn': 448242,
                 'Steven Souza': 519306,
                 'T.J. House': 543334,
                 'Pat McCoy': 519003,
                 'Wellington Castillo': 456078,
                 'Daniel Coulombe': 543056,
                 'Pedro Villareal': 543881,
                 'Juan Segura': 516416,
                 'C.J.L Riefenhauser': 592677,
                 'CC Sabathia': 282332,
                 'Giovanny Urshela': 570482,
                 'Vincent Velasquez': 592826,
                 'Steve Baron': 571467,
                 'Nori Aoki': 493114,
                 'Melvin Upton': 425834,
                 'Felipe Rivero':553878,
                 'Chin-hui Tsao': 425835,
                 'Mike Wright': 605541,
                 'Matt Boyd': 571510,
                 'Setphen Strasburg': 544931,
                 'Christian Adames': 542436,
                 'Daniel Winkler': 595465,
                 'Abel de los Santos': 593582,
                 'Fransicso Liriano': 434538,
                 'Charllie Morton': 450203,
                 'Ji-Man Choi': 596847,
                 'Seung Hwan Oh': 493200,
                 'Kalbe Cowart': 592230,
                 'Byung Ho Park': 666560,
                 'A.J. Reed': 607223,
                 'Yulieski Gurriel': 493329,
                 'Vicente Campos': 553883,
                 'Daniel Winkler': 595465,
                 'Albert Almora': 546991,
                 'John Gantr': 607231,
                 'JohnRyan Murphy': 571974,
                 'John Barbato': 592127,
                 'Max Strahm':  621381,
                 'Louie Varland': 686973,
                 'Guillermo Zuniga': 670871,
                 'Josh H. Smith': 669701,
                 'Jonny Deluca': 676356,
                 'Alfonso Rivas': 663845,
                 'Josh Palacios': 641943,
                 'Carl Edwards': 605218,
                 'Joseph Colon': 571572,
                 'J.T. Chargois': 608638,
                 'Charlie Tilson': 605508,
                 'R Jose Berrios': 621244,
                 'Jerad Eickhoffn': 595191,
                 'Steven Matx': 571927,
                 'Jake Junis': 596001,
                 'Jacob Faria': 607188,
                 'Cesar.R Puello': 527049,
                 'Alex Mejia': 622270,
                 'J.T. Riddle': 595375,
                 'J.P.L Crawford': 641487,
                 'Luis Robert': 673357,
                 'Daniel Lynch': 663738,
                 'Sebastian Rivero': 665861,
                 'Nate Lowe': 663993,
                 'Alexander Wells': 649144,
                 'Brent Honeywell Jr.': 641703,
                 'Josh Fuentes': 658069,
                 'Jazz Chisholm': 665862,
                 'Dwight Smith Jr': 596105,
                 'Mike Soroka': 647336,
                 'Andy Young': 670370,
                 'Matthew Festa': 670036,
                 'C.J. Abrams': 682928,
                 'Jackson Frazier': 640449,
                 'Mike Siani': 672279,
                 'LaMonte Wade Jr': 664774,
                 'Jean Carlos Mejia': 650496,
                 'Sammy Long': 669674,
                 'Jon Heasley': 669169,
                 'Jake Latz': 656641,
                 'Hoy Jun Park': 660829,
                 'Ronald Acuna': 660670,
                 'Lourdes Gurriel': 666971,
                 'Travis Lakins': 664042,
                 'Steve Wilkerson': 592859,
                 'Michael Brosseau': 670712,
                 'Peter Fairbanks': 664126,
                 'R Rosenthal': 572096,
                 'Franklin German': 681464,
                 'Gregory Weissert': 669711,
                 'Jose Lopez': 673111,
                 'Samuel Aldegheri': 691951,
                 'Nestor Cortes Jr.': 641482,
                 'Daniel Poncedeleon': 594965,
                 'R Karns': 501992,
                 'Tom Eshelman': 664045,
                 'D.J. Johnson': 597113,
                 'Shed Long': 643418,
                 'Matthew Barnes': 598264,
                 'LaMonte Wade': 664774,
                 'Aaron.R Civale': 650644,
                 'R Pineda': 501381,
                 'Donnie Walton': 622268,
                 'Jose  Lopez': 673111,
                 'Jonathon Feyereisen': 656420,
                 'Conner Memez': 669214,
                 'Chris Bostick': 607471,
                 'R Schultz.Jaime': 621289,
                 'C.D. Pelham': 641962,
                 'Brandon.L Woodruff': 605540,
                 'Robert Gsellmao': 607229,
                 'P.J.L Conlon': 664869,
                 'Asdrubel Cabrera': 452678,
                 'Duane Underwood': 621249,
                 'Taykor Cole': 518566,
                 'JonathanvHolder': 656547,
                 'Julioe Teheran': 527054,
                 'Yoan Moncado': 660162,
                 'Joshn Fogg': 150319,
                 'Hirkoi Kuroda':493133,
                 }
    with conn_pool.getconn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT player_name, player_id FROM players")
            for player_name, player_id in cur.fetchall():
                player_name = normalize_name(player_name)
                if player_name in aliases:
                    player_name = aliases[player_name]
                lookup[player_name] = player_id
    return lookup

player_lookup = load_player_lookup()

def parse_game(lines, game_id):
    state = GameStateTracker(game_id, player_lookup)
    for line in lines:
        tokens = line.strip().split(',')
        if line.startswith("start"):
            state.handle_start(tokens)
        elif line.startswith("sub"):
            state.handle_sub(tokens)
        elif line.startswith("play"):
            state.handle_play(tokens)
        elif line.startswith("radj"):
            state.handle_radj(tokens)
        elif line.startswith("padj"):
            state.handle_padj(tokens)
        elif line.startswith("badj"):
            state.handle_badj(tokens)

    return state 

def parse_info_block(game_lines):
    parsed = {}
    for line in game_lines:
        if line.startswith("info,"):
            _, key, value = line.strip().split(",", 2)
            key = key.strip().lower()
            value = value.strip()

            # Only parse what we care about
            if key == "date":
                parsed["date"] = value  # keep as string for conversion
            elif key == "starttime":
                parsed["starttime"] = value
            elif key == "site":
                parsed["site"] = value
            elif key == "timeofgame":
                parsed["timeofgame"] = value
            elif key == "hometeam":
                parsed["hometeam"] = value
            elif key == "visteam":
                parsed["visteam"] = value
            elif key == "usedh":
                parsed["usedh"] = value.lower() == "true"
            elif key == "attendance":
                try:
                    parsed["attendance"] = int(value)
                except ValueError:
                    parsed["attendance"] = None
            elif key == "fieldcond":
                parsed["fieldcond"] = value
            elif key.startswith("ump"):
                parsed[key] = value
                
            
    required_keys = ["date", "starttime", "hometeam", "visteam", "site"]
    for key in required_keys:
        if key not in parsed:
            raise ValueError(f"Missing required info: {key}")

    return parsed

def read_files(file_path):
    with open(file_path, 'r') as f:
        return [line.strip() for line in f]
    
def split_by_game(lines):
    games = []
    current_game = []

    for line in lines:
        if line.startswith("id,"):
            if current_game:
                games.append(current_game)
                current_game = []
        current_game.append(line)

    # Add the last game
    if current_game:
        games.append(current_game)

    return games


def process_single_game(game_lines, g_id=None):
    info = None
    conn = None
    try:
        info = parse_info_block(game_lines)
        utc_datetime = convert_to_utc(info["date"], info["starttime"], info["site"])
        
        conn = conn_pool.getconn()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT get_game_id_from_time(%s, %s, %s)
            """, (info["hometeam"], info["visteam"], utc_datetime))
            row = cur.fetchone()
            if row:
                game_id = row[0]
        if game_id is None:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT get_game_id_from_time(%s, %s, %s)
                """, (info["visteam"], info["hometeam"], utc_datetime))
                row = cur.fetchone()
                if row:
                    game_id = row[0]
            if game_id is None:
                if g_id is not None:
                    game_id = g_id
                else:
                    print(f"Game ID not found for {info['hometeam']} vs {info['visteam']} on {info['date']}", file=sys.stderr)
                    return
        # if game_id == 22669:
        #     tracker = parse_game(game_lines, game_id)

        #     with conn.cursor() as cur:
        #         cur.execute("""
        #             SELECT h_final_score, a_final_score FROM games WHERE game_id = %s
        #         """, (game_id,))
        #         scores = cur.fetchone()

        #     if scores:
        #         h_team_score, a_team_score = scores
        #         sim_home = tracker.get_home_score()
        #         sim_away = tracker.get_away_score()

        #         if sim_home != h_team_score or sim_away != a_team_score:
        #             print(f"Score mismatch for game {game_id}: simulated {sim_home}-{sim_away} vs actual {h_team_score}-{a_team_score}", file=sys.stderr)
        #             print(f'Home team: {info["hometeam"]}, Away team: {info["visteam"]}', file=sys.stderr)
        #             print(f'Game date: {info["date"]}, Start Time: {info["starttime"]}', file=sys.stderr)
        #     base_running = tracker.get_base_running_events()
        #     row = base_running[0]
        #     with conn.cursor() as cur:
        #         sql = """INSERT INTO base_running_events (game_id, inning_num, inning_half, at_bat_num, pitch_num, player, from_base, to_base, was_out, scored, stealing) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING"""
        #         print(sql)
        #         cur.execute(sql, row)
        tracker = parse_game(game_lines, game_id)

        base_states = tracker.get_base_states()
        base_running_events = tracker.get_base_running_events()
        pitch_metadata = tracker.get_pitch_metadata()
        comments = tracker.get_game_comments()
        game_subs = tracker.get_game_subs()
        fielding_sequences = tracker.get_fielding_sequences()
        defenses = tracker.get_defenses()
        lineups = tracker.get_lineups()
        home_defense = defenses['1']
        away_defense = defenses['0']
        home_lineup = lineups['1']
        away_lineup = lineups['0']
        defenses = home_defense + away_defense
        lineups = home_lineup + away_lineup

        with conn.cursor() as cur:
            try:
                for state in base_states:
                    cur.execute("""
                        INSERT INTO base_states (game_id, inning_num, inning_half, at_bat_num, pitch_num,  first, second, third)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",state)
                conn.commit()
            except Exception as e:
                print(f"Error inserting base states for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for event in base_running_events:
                    cur.execute("""
                        INSERT INTO base_running_events (game_id, inning_num, inning_half, at_bat_num, pitch_num, player, from_base, to_base, was_out, scored, stealing)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""",event)
                conn.commit()
            except Exception as e:
                print(f"Error inserting base running events for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for data in pitch_metadata:
                    cur.execute("""
                            UPDATE pitches SET home_score = %s, away_score = %s, outs = %s WHERE game_id = %s AND inning_num = %s AND inning_half = %s AND at_bat_num = %s AND pitch_num = %s""", data)
                conn.commit()
            except Exception as e:
                print(f"Error inserting pitch metadata for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for com in comments:
                    cur.execute("""
                        INSERT INTO game_com (game_id, inning_num, inning_half, at_bat_num, pitch_num, com)
                        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", com)
                conn.commit()    
            except Exception as e:
                print(f"Error inserting game comments for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for sub in game_subs:
                    cur.execute("""INSERT INTO substitutions (game_id, inning_num, inning_half, at_bat_num, pitch_num, sub_in, sub_out, lineup_spot, position) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", sub)
                conn.commit()
            except Exception as e:
                print(f"Error inserting game substitutions for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for seq in fielding_sequences:
                    cur.execute("""INSERT INTO fielding_sequences (fielding_sequence_id, game_id, inning_num, inning_half, at_bat_num, pitch_num, fielder_id, target_id, out_at, error_on, next_fielding_sequence_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", seq)
                conn.commit()
            except Exception as e:
                print(f"Error inserting fielding sequences for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for state in defenses:
                    cur.execute("""INSERT INTO defense_states (game_id, inning_num, inning_half, at_bat_num, pitch_num, pos_1, pos_2, pos_3, pos_4, pos_5, pos_6, pos_7, pos_8, pos_9, home) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", state)
                conn.commit()
            except Exception as e:
                print(f"Error inserting defense states for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()
            try:
                for lineup in lineups:
                    cur.execute("""INSERT INTO batting_lineups (game_id, inning_num, inning_half, at_bat_num, pitch_num, first, second, third, fourth, fifth, sixth, seventh, eighth, ninth, home) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING""", lineup)
                conn.commit()
            except Exception as e:
                print(f"Error inserting batting lineups for game {game_id}: {e}", file=sys.stderr)
                conn.rollback()

            print(f"Game {game_id} processed successfully.")

            
 

    except Exception as e:
        if info:
            print(f"Error in game ({info.get('hometeam')} vs {info.get('visteam')}, {info.get('date')}): {e}", file=sys.stderr)
        else:
            print(f"Error in game (Unknown Teams): {e}", file=sys.stderr)

    finally:
        if conn:
            conn_pool.putconn(conn)






with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []
    conn = None
    for subdir, dirs, files in os.walk(rootdir):
        for file in files:
            if file == 'unzip.py':
                continue
            
            filepath = os.path.join(subdir, file)
            lines = read_files(filepath)
            games = split_by_game(lines)
            # if file == '2023BAL.EVA':
            #     for game in games:
            #        process_single_game(game)
            #     sys.exit(0)


            season_year = file[0:4]
            team_abr = file[4:7].upper()
            if 'eve' in filepath:
                conn = conn_pool.getconn()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                                    SELECT game_id from games g
                                    join team_abbreviation_map hmap on g.venue_team_id = hmap.team_id where 
                                    level_id = 1 and
                                    type_id = 2 and
                                    hmap.external_abbr = %s and
                                    g.season_year = %s
                                    ORDER BY g.datetime
                                    """, (team_abr, season_year))
                        rows = cur.fetchall()
                    if rows:
                        game_ids = [row[0] for row in rows]
                

                
                        idx = 0
                        if len(games) > len(game_ids):
                            with conn.cursor() as cur:
                                cur.execute("""
                                            SELECT game_id from games g
                                            join team_abbreviation_map hmap on g.h_team_id = hmap.team_id where 
                                            level_id = 1 and
                                            type_id = 2 and
                                            hmap.external_abbr = %s and
                                            g.season_year = %s
                                            ORDER BY g.datetime
                                            """, (team_abr, season_year))
                                rows = cur.fetchall()
                            if rows:
                                game_ids = [row[0] for row in rows]
                            if len(games) > len(game_ids):
                                game_ids = None
                        elif len(games) < len(game_ids):
                            print(f'Less games than game_ids for {filepath}')
                finally:
                    conn_pool.putconn(conn)
                if game_ids: 
                    for game in games:
                        futures.append(executor.submit(process_single_game, game, game_ids[idx]))
                        idx += 1
                else:
                    for game in games:
                        futures.append(executor.submit(process_single_game, game))
            else:
                for game in games:
                    futures.append(executor.submit(process_single_game, game))
           

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Thread failed: {e}", file=sys.stderr)



    