from psycopg2 import pool
import passwords
import os
import threading
import concurrent.futures


rootdir = "C:/Users/johnb/Desktop/Sports/mlb/src/Play-By-Play"

conn_pool = pool.SimpleConnectionPool(5, 95, dbname='mlb_data', user="postgres", password=f"{passwords.password}", host="127.0.0.1", port="5432")

def read_files(file_path):
    pbp = []
    with open(file_path, 'r') as f:
        pbp = f.readlines()

    pbp = [line.strip() for line in pbp]
    
    return pbp


for subdir, dirs, files in os.walk(rootdir):
    for file in files:
        print(os.path.join(subdir, file))
        filepath = os.path.join(subdir, file)
        contents = read_files(filepath)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future = executor.submit(read_files, filepath)
            result = future.result()

    