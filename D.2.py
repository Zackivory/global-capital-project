"""
get batch result and save into batch_output folder
"""

# Read the job_ids from the job_ids.json file
import json
import os

from openai import OpenAI
import pandas as pd
import time
import sqlite3

# Connect to SQLite database
db_path = os.path.join(os.getcwd(), "news_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get undownloaded job_ids from the table
cursor.execute('''
SELECT job_id FROM batch_jobs WHERE is_downloaded = FALSE AND job_id IS NOT NULL
''')
job_ids = [row[0] for row in cursor.fetchall()]

# Close the database connection

client = OpenAI()
fail_flag = False
finished = set()
while True:
    for job_id in job_ids:
        job = client.batches.retrieve(job_id)
        if job.status == "failed":
            # If any of the jobs failed we will stop and check it up.
            print(f"Job {job_id} has failed with error {job.errors}")
            fail_flag = True
            break
        elif job.status == 'in_progress':
            print(
                f'Job {job_id} is in progress, {job.request_counts.completed}/{job.request_counts.total} requests completed')
        elif job.status == 'finalizing':
            print(f'Job {job_id} is finalizing, waiting for the output file id')
        elif job.status == "completed":
            print(f"Job {job_id} has finished")
            finished.add(job_id)
        else:
            print(f'Job {job_id} is in status {job.status}')

    if fail_flag == True or len(finished) == len(job_ids):
        break
    time.sleep(60)

# Check for failed embeddings
for job_id in job_ids:
    job = client.batches.retrieve(job_id)
    print(f'{job.request_counts.failed}/{job.request_counts.total} requests failed in job {job_id}')

# Create batch_output folder if it does not exist
if not os.path.exists('batch_output'):
    os.makedirs('batch_output')


# Download the output files one by one
for job_id in job_ids:

    job = client.batches.retrieve(job_id)
    output_file_id = job.output_file_id
    output_file = client.files.content(output_file_id).text

    # Extract the id and embedding
    embedding_results = []
    for line in output_file.split('\n')[:-1]:
        data = json.loads(line)
        id = data.get('custom_id')  # Use 'id' instead of 'custom_id'
        embedding = data['response']['body']['data'][0]['embedding']
        embedding_results.append([id, embedding])

    # Save the embeddings to a CSV file
    embedding_results_df = pd.DataFrame(embedding_results, columns=['id', 'embedding'])
    embedding_results_df.to_csv(f'./batch_output/{job_id}.csv', index=False)

    # Mark the job as downloaded
    cursor.execute('''
    UPDATE batch_jobs
    SET is_downloaded = TRUE
    WHERE job_id = ?
    ''', (job_id,))
    conn.commit()
    
conn.close()
if __name__ == '__main__':
    print()