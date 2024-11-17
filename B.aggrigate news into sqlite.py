import os
import sqlite3
import pandas as pd
import os



def is_file_processed(file_name):
    if not os.path.exists(processed_files):
        return False
    with open(processed_files, "r") as f:
        processed = [line for line in f.read().splitlines()]
    return file_name in processed

def mark_file_as_processed(file_name):
    with open(processed_files, "a") as f:
        f.write(f"{file_name}\n")


if __name__ == '__main__':
    processed_files = os.path.join(os.getcwd(), "processed_files.txt")
    if not os.path.exists(processed_files):
        with open(processed_files, "w") as f:
            pass


    total_token_count = 0
    db_path = os.path.join(os.getcwd(), "news_data.db")
    for root, dirs, files in os.walk('news_data'):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                if not is_file_processed(file_path):
                    try:
                        # Extract date part from the file name to use as part of the ID
                        date_part = os.path.basename(file_path).split('_')[-1].split('.')[0]

                        # Connect to SQLite database (or create it if it doesn't exist)
                        with sqlite3.connect(db_path) as conn:
                            cursor = conn.cursor()

                            # Create table if it doesn't exist
                            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS news (
                                id TEXT PRIMARY KEY,
                                datetime TEXT,
                                source TEXT,
                                title TEXT,
                                content TEXT
                            )
                            ''')

                            # Read the CSV file into a DataFrame
                            df = pd.read_csv(file_path)

                            # Insert data into the SQLite database
                            for _, row in df.iterrows():
                                # Create a unique ID by combining the date part of the file name and the id from the CSV
                                unique_id = f"{date_part}_{row['id']}"
                                cursor.execute('''
                                INSERT INTO news (id, datetime, source, title, content)
                                VALUES (?, ?, ?, ?, ?)
                                ''', (unique_id, row['datetime'], row['source'], row['title'], row['content']))

                            # Commit the transaction
                            conn.commit()
                        print(f"Processed file: {file_path}")

                        mark_file_as_processed(file_path)
                    except Exception as e:
                        print(f"Error processing file {file_path}: {e}")