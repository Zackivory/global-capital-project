"""
create batch jobs from news_data.db
call openai embedding batch api
"""
import os
import json
import pandas as pd
import sqlite3
from openai import OpenAI

# Check if the data folder exists, else create it
def check_and_create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")
    else:
        print(f"Folder '{folder_path}' already exists.")

check_and_create_folder('./batch_files')

# Connect to SQLite database
db_path = os.path.join(os.getcwd(), "news_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Read data from the database
query = "SELECT id, content FROM news"
df = pd.read_sql_query(query, conn)

# Close the database connection
conn.close()

# Step 2: Create batch files
# Create the batch files
batch_size = 10000
batch_file = df.copy()
batch_file_name = 'news_data_batch'
num_files = len(batch_file) // batch_size + 1
print(f"Number of files to create: {num_files}")

for num_file in range(num_files):
    output_file = f'./batch_files/{batch_file_name}_part{num_file}.jsonl'
    # Make sure that the file does not exist, so don't add to an existing file
    if os.path.exists(output_file):
        os.remove(output_file)
    # Write each embedding entry to a new line
    with open(output_file, 'a') as file:
        for index, row in batch_file.iloc[batch_size*num_file : min(batch_size*(num_file+1), len(batch_file))].iterrows():
            payload = {
                "custom_id": row['id'],
                "method": "POST",
                "url": "/v1/embeddings",
                "body": {
                    "input": row["content"],
                    "model": "text-embedding-3-small",
                    "encoding_format": "float",
                    "dimensions": 1536
                }
    
            }
            file.write(json.dumps(payload) + '\n')

    # Sanity check, print the first 2 lines
    with open(output_file, 'r') as file:
        for line in file.readlines()[:2]:
            print(line)






if __name__ == '__main__':
    print()