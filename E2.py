"""
create a table of the name of output in the batch_output folder, keep track whether is already enter into zilliz
for all output in batch_output, use id to get related info from news table, joint into a df and upload to zilliz

"""
import os
import sqlite3
import json
import sqlite3
import pandas as pd

import configparser
import pandas as pd
import time
import random

from numpy.matlib import empty
from openai import OpenAI
from pymilvus import connections, utility
from pymilvus import Collection, DataType, FieldSchema, CollectionSchema





def add_to_zilliz(collection_name, df):
    """
    :param collection_name: The name of the collection to be created or reset(should be ai_name with _to replace space)
    :return: Total tokens used
    """
   
        
    # Insert data
    # Function to read all .jsonl files from a folder
    total_token_count = 0
    to_insert = []
    # connect to milvus
    config = configparser.ConfigParser()
    # Read the configuration file with UTF-8 encoding
    with open('dev.ini', 'r', encoding='utf-8') as configfile:
        config.read_file(configfile)
    milvus_uri = config.get('milvus', 'uri')
    token = config.get('milvus', 'token')
    connections.connect("default",
                        uri=milvus_uri,
                        token=token)
    print(f"Connecting to DB: {milvus_uri}")
    # Check if the collection exists

    check_collection = utility.has_collection(collection_name)

    # create a collection with customized primary field: book_id_field
    dim = 1536
    fields = [
        FieldSchema(name='id', dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name='sqlite_id', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='datetime', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='source', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='title', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='content', dtype=DataType.VARCHAR, max_length=8192),
        FieldSchema(name='content_embedding', dtype=DataType.FLOAT_VECTOR, dim=dim)
    ]
    schema = CollectionSchema(fields, description='personal_qa')
    print(f"Creating example collection: {collection_name}")
    collection = Collection(name=collection_name, schema=schema)

    print(f"Schema: {schema}")
    print("Success!")
    # Create an index
    index_params = {
        'index_type': 'IVF_FLAT',
        'metric_type': 'L2',
        'params': {'nlist': 1024}
    }
    collection.create_index(field_name="content_embedding", index_params=index_params)
    # Fetch news from csv file
    df = df.drop(columns=['is_inserted_to_zilliz'])
    df = df.dropna(subset=['content_embedding'])
    df = df.fillna('')
    if df.empty:
        connections.disconnect("default")
        return df
    print(df)
    for index, row in df.iterrows():
        content_embedding = json.loads(row['content_embedding'])
        record = {
            'sqlite_id': row['id'],  # Ensure this matches the expected type
            'datetime': row['datetime'],  # Ensure this is a string if VARCHAR is expected
            'source': row['source'],
            'title': row['title'],
            'content': row['content'],
            'content_embedding': content_embedding
        }
        to_insert.append(record)
    print("Total Tokens Used:", total_token_count)
    collection.insert(to_insert)
    # flush
    print("Flushing...")
    start_flush = time.time()
    collection.flush()
    end_flush = time.time()
    print(f"Succeed in {round(end_flush - start_flush, 4)} seconds!")
    # load collection
    t0 = time.time()
    print("Loading collection...")
    collection.load()
    t1 = time.time()
    print(f"Succeed in {round(t1 - t0, 4)} seconds!")
    connections.disconnect("default")
    return df

# Connect to the SQLite database
conn = sqlite3.connect('news_data.db')
cursor = conn.cursor()

# Create a table to keep track of batch output files and their status
cursor.execute('''
CREATE TABLE IF NOT EXISTS batch_output_files (
    file_name TEXT PRIMARY KEY,
    is_inserted_to_zilliz BOOLEAN DEFAULT FALSE
)
''')
conn.commit()

# Get the list of files in the batch_output folder
batch_output_folder = 'batch_output'
batch_files = os.listdir(batch_output_folder)

# Insert the batch output files into the table if they are not already present
for file in batch_files:
    cursor.execute('''
    INSERT OR IGNORE INTO batch_output_files (file_name)
    VALUES (?)
    ''', (file,))
conn.commit()

# Close the database connection
conn.close()

import json
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('news_data.db')
cursor = conn.cursor()


# Process each batch output file
for file in batch_files:
    print(file)
    if file.endswith('.csv'):
        file_path = os.path.join(batch_output_folder, file)
        df = pd.read_csv(file_path)
        
        # Get related info from the news table
        ids = tuple(df['id'].tolist())
        query = f"SELECT * FROM news WHERE id IN ({','.join(['?']*len(ids))})"
        news_df = pd.read_sql_query(query, conn, params=ids)
        
        # Join the dataframes on the 'id' column
        merged_df = pd.merge(df, news_df, on='id')
        merged_df = merged_df.drop(columns=['content_embedding'])
        merged_df.rename(columns={'embedding': 'content_embedding'}, inplace=True)
        merged_df.to_csv('debug.csv', index=False)

        try:
            add_to_zilliz(collection_name='news_data', df=merged_df)
            # Update the status of the batch output file in the database
            cursor.execute('''
            UPDATE batch_output_files
            SET is_inserted_to_zilliz = TRUE
            WHERE file_name = ?
            ''', (file,))
            conn.commit()
        except Exception as e:
            print(f"Error processing file {file}: {e}")
            conn.rollback()

# Close the database connection
conn.close()
if __name__ == '__main__':
    print()