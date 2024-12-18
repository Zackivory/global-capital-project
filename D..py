# step 3 run the batch jobs
# create the files for the batch job
import json
import os

import pandas as pd
from openai import OpenAI

client = OpenAI()
batch_folder = './batch_files'
batch_input_files = []

# Process one file at a time
import sqlite3

# Connect to SQLite database
db_path = os.path.join(os.getcwd(), "news_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS batch_jobs (
    file_name TEXT PRIMARY KEY,
    job_id TEXT,
    is_downloaded BOOLEAN DEFAULT FALSE
    
)
''')

# Add all file names in batch folder to the table with empty job_id
for file in os.listdir(batch_folder):
    cursor.execute('''
    INSERT OR IGNORE INTO batch_jobs (file_name, job_id)
    VALUES (?, NULL)
    ''', (file,))
conn.commit()

# Process files with empty job_id
cursor.execute('''
SELECT file_name FROM batch_jobs WHERE job_id IS NULL
''')
files_to_process = cursor.fetchall()

job_creations = []
for (file,) in files_to_process:
    with open(f'{batch_folder}/{file}', "rb") as f:
        batch_input_file = client.files.create(
            file=f,
            purpose="batch"
        )
        batch_input_files.append(batch_input_file)

    # Create the batch job for the current file
    batch_file_id = batch_input_file.id
    job_creation = client.batches.create(
        input_file_id=batch_file_id,
        endpoint="/v1/embeddings",
        completion_window="24h",  # currently only 24h is supported
        metadata={
            "description": f"part_{file}_icd_embeddings"
        }
    )
    print(job_creation)
    job_creations.append(job_creation)

    # Update the job_id for the file in the database
    cursor.execute('''
    UPDATE batch_jobs
    SET job_id = ?
    WHERE file_name = ?
    ''', (job_creation.id, file))
    conn.commit()

# Close the database connection
conn.close()

if __name__ == '__main__':
    print()