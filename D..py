# step 3 run the batch jobs
# create the files for the batch job
import json
import os

import pandas as pd
from openai import OpenAI

client = OpenAI()
batch_folder = './batch_files'
batch_input_files = []
for file in os.listdir(batch_folder):
    batch_input_files.append(client.files.create(
        file=open(f'{batch_folder}/{file}', "rb"),
        purpose="batch"
    ))

# create the batch job
batch_file_ids = [batch_file.id for batch_file in batch_input_files]  # we get the ids of the batch files
job_creations = []
for i, file_id in enumerate(batch_file_ids):
    job_creations.append(client.batches.create(
        input_file_id=file_id,
        endpoint="/v1/embeddings",
        completion_window="24h",  # currently only 24h is supported
        metadata={
            "description": f"part_{i}_icd_embeddings"
        }
    ))

# We can see here the jobs created, they start with validation
for job in job_creations:
    print(job)

# Step 4: Monitor the batch jobs


# we extract the ids for the jobs
job_ids = [job.id for job in job_creations]
# Save the job_ids to a file for future reference
job_ids_file = 'job_ids.json'
with open(job_ids_file, 'w') as f:
    json.dump(job_ids, f)


if __name__ == '__main__':
    print()