import os
import pandas as pd
import re
from sqlalchemy import create_engine

# --- Configuration ---
# Directory containing your unzipped CSV files
CSV_FOLDER_PATH = 'cafe_data'
# PostgreSQL connection details
DB_USER = 'themetasetter'
DB_PASSWORD = 'themetasetter'
DB_HOST = 'localhost' # or your server IP
DB_PORT = '5432'
DB_NAME = 'cafebranddb'

def clean_table_name(filename):
    """Cleans a CSV filename to create a valid SQL table name."""
    # Remove the .csv extension
    name = filename.replace('.csv', '')
    # Replace invalid characters (like #) with underscores
    name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    # Convert to lowercase and remove leading/trailing underscores
    return name.lower().strip('_')

def main():
    """
    Main function to connect to the database, read CSVs, 
    and load them into PostgreSQL tables.
    """
    print("Starting data ingestion script...")
    
    # Create a database connection engine using SQLAlchemy
    engine_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    try:
        engine = create_engine(engine_url)
        print("Successfully connected to the PostgreSQL database.")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return

    # Iterate over all files in the specified folder
    for filename in os.listdir(CSV_FOLDER_PATH):
        if filename.endswith('.csv'):
            file_path = os.path.join(CSV_FOLDER_PATH, filename)
            table_name = clean_table_name(filename)
            
            print(f"\nProcessing '{filename}'...")
            print(f"Target table: '{table_name}'")
            
            try:
                # Read the CSV file into a pandas DataFrame
                # IMPORTANT: We specify ';' as the delimiter based on your file metadata
                df = pd.read_csv(file_path, delimiter=';')
                
                # Use pandas to_sql to write the DataFrame to the database
                # 'replace' will drop the table if it already exists and create a new one.
                # This is perfect for a repeatable demo script.
                df.to_sql(table_name, engine, if_exists='replace', index=False)
                
                print(f"Successfully loaded {len(df)} rows into '{table_name}'.")

            except Exception as e:
                print(f"Failed to load '{filename}'. Error: {e}")

    print("\nAll CSV files have been processed. Ingestion complete.")

if __name__ == "__main__":
    main()