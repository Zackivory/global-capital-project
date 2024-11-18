"""
read all csv in batch_output folder
copy the batch embeded float into news_data.db using
"""
import os
import json
import pandas as pd
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('news_data.db')
c = conn.cursor()

# Read all CSV files in the batch_output folder
batch_output_folder = 'batch_output'
for csv_file in os.listdir(batch_output_folder):
    if csv_file.endswith('.csv'):
        file_path = os.path.join(batch_output_folder, csv_file)
        df = pd.read_csv(file_path)
        print(f"Processing file: {file_path}")
        print(df.head())
        # Insert or update embeddings in the database
        for _, row in df.iterrows():
            id = row['id']
            embedding = row['embedding']  # Convert embedding to JSON string
            try:
                c.execute('''
                    UPDATE news
                    SET content_embedding = ?
                    WHERE id = ?
                ''', (embedding, id))
            except Exception as e:
                print(f"Error updating ID {id} with embedding: {e}")

# Commit changes and close the connection
conn.commit()
conn.close()
if __name__ == '__main__':
    print()

