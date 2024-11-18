"""
read from
"""
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

client = OpenAI()

def embed_with_tokens(text):
    response = client.embeddings.create(
        input=text,
        model='text-embedding-3-large'
    )
    embedding = response.data[0].embedding
    total_tokens = response.usage.total_tokens
    print(total_tokens)
    return embedding, total_tokens

def update_status_in_sqlite(df):
    conn = sqlite3.connect('news_data.db')
    c = conn.cursor()
    for index, row in df.iterrows():
        c.execute('''UPDATE news SET is_inserted_to_zilliz = ? WHERE id = ?''', (True, row['id']))
    conn.commit()
    conn.close()
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
    dim = 3072
    fields = [
        FieldSchema(name='id', dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name='sqlite_id', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='datetime', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='source', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='title', dtype=DataType.VARCHAR, max_length=2048),
        FieldSchema(name='content', dtype=DataType.VARCHAR, max_length=2048),
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


if __name__ == '__main__':
    collection_name = 'news_data'

    # Connect to the SQLite database
    conn = sqlite3.connect('news_data.db')
    c = conn.cursor()

    # Read 100 lines at a time from the news table
    offset = 0
    batch_size = 1000
    while True:
        query = f"SELECT * FROM news LIMIT {batch_size} OFFSET {offset}"
        df = pd.read_sql_query(query, conn)
        if df.empty:
            break
        added_df=add_to_zilliz(collection_name, df)
        update_status_in_sqlite(added_df)
        offset += batch_size

    # Close the connection
    conn.close()
    import os

