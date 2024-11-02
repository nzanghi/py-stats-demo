import sqlite3
import polars as pl
from concurrent.futures import ThreadPoolExecutor
import os


# Function that will read CSV and insert the data from it into a sqlite db using polars
def load_csv_to_sqlite(file_path, db_path, table_name):
    try:
        # Open a new connection in each thread
        conn = sqlite3.connect(db_path)

        # Read the CSV into a Polars DataFrame (keeps NULL/empty fields)
        df = pl.read_csv(file_path, null_values=["", "NULL"])

        # Prepare columns for insert, escaping column names with special characters
        columns = ", ".join([f"[{col}]" for col in df.columns])
        placeholders = ", ".join("?" * len(df.columns))

        # Create a table (if it does not already exist)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        conn.execute(create_table_query)

        # Insert the rows
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        conn.executemany(insert_query, df.to_numpy().tolist())

        # Commit changes and close the connection
        conn.commit()
        conn.close()

        print(f"Successfully loaded {file_path} into {table_name}")
    except Exception as e:
        print(f"Error loading {file_path}: {e}")


# Function to handle multithreading
def load_multiple_csvs(csv_folder, db_path):
    # Get a list of .csv files in the folder
    csv_files = [
        os.path.join(csv_folder, f)
        for f in os.listdir(csv_folder)
        if f.endswith(".csv")
    ]

    # Define the thread pool executor to read files concurrently
    with ThreadPoolExecutor() as executor:
        # Submit tasks to the executor
        for csv_file in csv_files:
            table_name = os.path.splitext(os.path.basename(csv_file))[
                0
            ]  # Table name based on the file name
            executor.submit(load_csv_to_sqlite, csv_file, db_path, table_name)


if __name__ == "__main__":
    # Define the folder containing .csv files and the sqlite database path
    csv_folder_path = os.path.join("input")
    sqlite_db_path = os.path.join("db", "stats.db")

    # Load multiple csv files into a sqlite db
    load_multiple_csvs(csv_folder_path, sqlite_db_path)
