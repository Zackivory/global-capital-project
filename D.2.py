"""
get batch result and save into batch_output folder
"""

# Read the job_ids from the job_ids.json file
import json
from openai import OpenAI
import pandas as pd
import time
with open('job_ids.json', 'r') as f:
    job_ids = json.load(f)
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

# Step 5: Download the output files
# Extract the output file ids
output_files_ids = []
for job_id in job_ids:
    output_files_ids.append(client.batches.retrieve(job_id).output_file_id)

# Read the output files
output_files = []
for output_file_id in output_files_ids:
    output_file = client.files.content(output_file_id).text
    output_files.append((output_file_id, output_file))

# Extract the id and embedding
import os

# Create batch_output folder if it does not exist
if not os.path.exists('batch_output'):
    os.makedirs('batch_output')

for file_id, file_content in output_files:
    print(file_id)
    embedding_results = []
    for line in file_content.split('\n')[:-1]:
        data = json.loads(line)

        id = data.get('custom_id')  # Use 'id' instead of 'custom_id'
        embedding = data['response']['body']['data'][0]['embedding']
        embedding_results.append([id, embedding])
    embedding_results = pd.DataFrame(embedding_results, columns=['id', 'embedding'])
    embedding_results.to_csv(f'./batch_output/{file_id}.csv', index=False)

if __name__ == '__main__':
    print()