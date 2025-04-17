import concurrent.futures
from psycopg2 import pool
import passwords

# Create a connection pool
conn_pool = pool.SimpleConnectionPool(
    5, 95, 
    dbname="mlb_data", 
    user="postgres", 
    password=f"{passwords.password}", 
    host="127.0.0.1", 
    port="5432"
)

def process_batch(batch_id, batch_size):
    """Process a batch of records to delete non-final games"""
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        # Get a batch of IDs
        offset = batch_id * batch_size
        cursor.execute("""
            SELECT game_id FROM game_info 
            WHERE game_data->>'game_status' != 'F' OR game_data->>'game_status' IS NULL
            ORDER BY game_id
            LIMIT %s OFFSET %s
        """, (batch_size, offset))
        
        ids_to_delete = [row[0] for row in cursor.fetchall()]
        
        if ids_to_delete:
            # Use a parameterized query with multiple values
            placeholders = ','.join(['%s'] * len(ids_to_delete))
            cursor.execute(f"""
                DELETE FROM game_info 
                WHERE game_id IN ({placeholders})
            """, ids_to_delete)
            
            conn.commit()
            print(f"Batch {batch_id}: Deleted {cursor.rowcount} records")
        
        return len(ids_to_delete)
    except Exception as e:
        conn.rollback()
        print(f"Error in batch {batch_id}: {e}")
        return 0
    finally:
        cursor.close()
        conn_pool.putconn(conn)

def cleanup_games():
    # First, get the total count to determine number of batches
    conn = conn_pool.getconn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM game_info 
            WHERE game_data->>'game_status' != 'F' OR game_data->>'game_status' IS NULL
        """)
        total_count = cursor.fetchone()[0]
        print(f"Found {total_count} games to clean up")
        
        batch_size = 5000
        num_batches = (total_count + batch_size - 1) // batch_size  # Ceiling division
        
        # Process in parallel using thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            futures = [
                executor.submit(process_batch, batch_id, batch_size) 
                for batch_id in range(num_batches)
            ]
            
            deleted_count = 0
            for future in concurrent.futures.as_completed(futures):
                deleted_count += future.result()
            
            print(f"Cleanup complete. Deleted {deleted_count} games.")
            
    except Exception as e:
        print(f"Error in cleanup: {e}")
    finally:
        cursor.close()
        conn_pool.putconn(conn)

if __name__ == "__main__":
    try:
        cleanup_games()
    finally:
        conn_pool.closeall()