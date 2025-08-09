import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# --- Configuration ---
# Match these details with your load_data.py script
DB_USER = 'conquerormikrokosmos'
DB_PASSWORD = ''
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'cafebranddb' # The database to be dropped and recreated

# Connect to the default 'postgres' database to manage other databases
# You CANNOT be connected to the database you want to drop.
DSN = f"user={DB_USER} password={DB_PASSWORD} host={DB_HOST} port={DB_PORT} dbname=postgres"

def main():
    """
    Connects to the PostgreSQL server, drops the specified database if it exists,
    and then creates a fresh, empty one.
    """
    print("Starting database cleanup script...")
    conn = None
    try:
        # Establish a connection to the server
        conn = psycopg2.connect(DSN)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # IMPORTANT: Allows DROP DB
        
        cursor = conn.cursor()
        
        print(f"Checking for existing database '{DB_NAME}'...")
        
        # SQL to check if the database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        
        if exists:
            print(f"-> Database '{DB_NAME}' found. Dropping it now...")
            # Use f-string safely here as DB_NAME is from our config, not user input
            cursor.execute(f'DROP DATABASE "{DB_NAME}" WITH (FORCE);')
            print(f"Successfully dropped database '{DB_NAME}'.")
        else:
            print(f"-> Database '{DB_NAME}' not found. Skipping drop.")

        # Create a fresh database
        print(f"Creating new empty database '{DB_NAME}'...")
        cursor.execute(f'CREATE DATABASE "{DB_NAME}";')
        
        # Grant privileges to the user on the new database
        cursor.execute(f'GRANT ALL PRIVILEGES ON DATABASE "{DB_NAME}" TO {DB_USER};')
        print(f"Successfully created database '{DB_NAME}' and granted privileges.")

        cursor.close()

    except psycopg2.Error as e:
        print(f"A database error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Connection closed.")
    
    print("\nCleanup complete. The database is now fresh and ready.")

if __name__ == "__main__":
    main()