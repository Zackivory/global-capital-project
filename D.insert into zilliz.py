import configparser
import pandas as pd
import time
import random
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


def add_to_zilliz(collection_name, file_path):
    """
    :param collection_name: The name of the collection to be created or reset(should be ai_name with _to replace space)
    :return: Total tokens used
    """
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
    if check_collection:
        print(f"Collection {collection_name} already exists. Skipping creation.")
    else:
        print("Success!")
        # create a collection with customized primary field: book_id_field
        dim = 3072
        fields = [
        FieldSchema(name='id', dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name='date', dtype=DataType.VARCHAR, max_length=2048),
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
        
    # Insert data
    # Function to read all .jsonl files from a folder
    total_token_count = 0
    to_insert = []
    # Fetch news from csv file
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        content_embedding, total_tokens = embed_with_tokens(row['content'])
        total_token_count += total_tokens
        print(f"Total accumulated tokens used: {total_token_count}")
        record = {
            'content': row['content'],
            'source': row['source'],
            'title': row['title'],
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
    return total_token_count


if __name__ == '__main__':
    import os

    processed_files = "processed_files.txt"

    def is_file_processed(file_name):
        if not os.path.exists(processed_files):
            return False
        with open(processed_files, "r") as f:
            processed = [line.split(',')[0] for line in f.read().splitlines()]
        return file_name in processed

    def mark_file_as_processed(file_name, total_token_count):
        with open(processed_files, "a") as f:
            f.write(f"{file_name},{total_token_count}\n")

    total_token_count = 0
    for root, dirs, files in os.walk('news_data'):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                if not is_file_processed(file_path):
                    total_token_count += add_to_zilliz(collection_name="news", file_path=file_path)
                    print(f"Processed file: {file_path}")
                    print(f"Total tokens used: {total_token_count}")
                    mark_file_as_processed(file_path,total_token_count)
    print(f"Total tokens used: {total_token_count}")
