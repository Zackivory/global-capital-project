import json
import configparser
import random
import sqlite3
from pprint import pprint

import pandas as pd
import tushare as ts
from openai import OpenAI

import requests
client = OpenAI()

config_file = f'dev.ini'

config = configparser.ConfigParser()
config.read(config_file)
milvus_uri = config.get('milvus', 'uri')
milvus_token = config.get('milvus', 'token')


def embed_with_tokens(text):
    response = client.embeddings.create(
        input=text,
        model='text-embedding-3-small'
    )
    embedding = response.data[0].embedding
    total_tokens = response.usage.total_tokens
    print(total_tokens)
    return embedding, total_tokens


def form_header_to_zilliz(human_comments):
    embedding, total_tokens = embed_with_tokens(human_comments)
    # Define the endpoint and token
    CLUSTER_ENDPOINT = milvus_uri
    TOKEN = milvus_token
    # Define the request URL
    url = f"{CLUSTER_ENDPOINT}/v2/vectordb/entities/search"
    # Define the headers
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "accept": "application/json",
        "content-type": "application/json"
    }
    return embedding, headers, url


def search_similar_history_news( embedding, url, headers,current_date_time):
    data = {
        "collectionName": "news_data",
        "data": [embedding],
        "filter": f'datetime <= "{current_date_time}"',
        "limit": 3,
        "annsField": "content_embedding",
        "outputFields": [
            "content",
            "sqlite_id",
            "datetime"
        ]
    }

    # Make the POST request
    response = requests.post(url, headers=headers, data=json.dumps(data))

    # Check the response
    if response.status_code == 200:
        return response.json()
    else:
        return None
def get_random_date(list_dates):

    # Select one date randomly
    selected_date = random.choice(list_dates)

    print(f"Randomly selected date: {selected_date}")

    return selected_date
def get_stock_list_data():
    # Connect to SQLite database
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()

    # Fetch the stock list data
    query = "SELECT ts_code FROM stock_list_data"
    cursor.execute(query)
    stock_list = cursor.fetchall()

    # Close the database connection
    conn.close()

    # Return the list of stock codes
    return [stock[0] for stock in stock_list]


def get_list_dates():
    start_date = pd.to_datetime('20230515')
    end_date = pd.to_datetime('20241115')
    return pd.date_range(start_date, end_date).strftime('%Y%m%d').tolist()

def get_closing_price(ts_code, date,is_from_db=True):
    """
    Get the closing price for a given stock code and date.
    """
    date_str = pd.to_datetime(date)
    date_str = date_str.strftime('%Y%m%d')
    if is_from_db:
        # Connect to the SQLite database
        conn = sqlite3.connect('stock_data.db')
        cursor = conn.cursor()

        # Fetch the closing price from the database
        query = "SELECT close_hfq FROM historical_stock_price_data WHERE ts_code = ? AND trade_date = ?"
        for _ in range(20):
            print(date_str)
            cursor.execute(query, (ts_code, date_str))
            result = cursor.fetchone()
            print(result)
            if result:
                break
            # If no result, try the previous day's date
            date_str = pd.to_datetime(date_str) - pd.Timedelta(days=1)
            date_str = date_str.strftime('%Y%m%d')

        # Close the database connection
        conn.close()

        if result:
            return pd.DataFrame({'close_hfq': [result[0]]})
        else:
            raise ValueError(f"No closing price found for stock code {ts_code} on or after {date}")
    else:
        # Connect to the Tushare API
        config = configparser.ConfigParser()
        config.read('dev.ini')
        ts_token = config.get('ts', 'token')
        pro = ts.pro_api(token=ts_token)

        # Initialize depth counter
        depth = 0

        while depth < 20:
            # Fetch the closing price
            df = pro.stk_factor(ts_code=ts_code, start_date=date, end_date=date, fields='close_hfq')

            if not df.empty:
                return df
            else:
                print(date)
                date = pd.to_datetime(date) - pd.Timedelta(days=1)
                date = date.strftime('%Y%m%d')
                depth += 1

        raise ValueError(f"No closing price found for stock code {ts_code} within 20 days before {date}")

def get_next_day_opening_price(ts_code, date,is_from_db=True):
        """
        Get the next day's opening price percentage change for a given stock code and date.

        :param ts_code: The stock code.
        :param date: The date in 'YYYYMMDD' format.
        :return: The next day's opening price.
        """
        if is_from_db:
            # Connect to the SQLite database
            conn = sqlite3.connect('stock_data.db')
            cursor = conn.cursor()
            next_day_str = pd.to_datetime(date) + pd.Timedelta(days=1)
            next_day_str = next_day_str.strftime('%Y%m%d')
            # Fetch the next day's opening price from the database
            query = "SELECT open_hfq FROM historical_stock_price_data WHERE ts_code = ? AND trade_date = ?"
            for _ in range(20):
                print(next_day_str)
                cursor.execute(query, (ts_code, next_day_str))
                result = cursor.fetchone()
                print(result)

                if result:
                    break
                # If no result, try the next day's date
                next_day = pd.to_datetime(next_day_str) + pd.Timedelta(days=1)
                next_day_str = next_day.strftime('%Y%m%d')

            # Close the database connection
            conn.close()

            if result:
                return pd.DataFrame({'open_hfq': [result[0]]})
            else:
                raise ValueError(f"No opening price found for stock code {ts_code} on or after {date}")

                
        else:
            # Connect to the Tushare API
            config = configparser.ConfigParser()
            config.read('dev.ini')
            ts_token = config.get('ts', 'token')
            pro = ts.pro_api(token=ts_token)

            # Initialize depth counter
            depth = 0

            while depth < 20:
                # Get the next day's date
                next_day = pd.to_datetime(date) + pd.Timedelta(days=1)
                next_day_str = next_day.strftime('%Y%m%d')

                # Fetch the next day's opening price
                df = pro.stk_factor(ts_code=ts_code, start_date=next_day_str, end_date=next_day_str, fields='open_hfq')

                if not df.empty:
                    return df
                else:
                    print(next_day_str)
                    date = next_day_str
                    depth += 1

            raise ValueError(f"No opening price found for stock code {ts_code} within 20 days after {date}")

def get_next_day_opening_price_percentage_change(ts_code, date):
    next_day_opening_price = get_next_day_opening_price(ts_code, date)
    next_day_opening_price_percentage_change = (next_day_opening_price['open_hfq'].values[0]- get_closing_price(ts_code, date)['close_hfq'].values[0]) / get_closing_price(ts_code, date)['close_hfq'].values[0]
    return next_day_opening_price_percentage_change

def get_current_news(date,limit=0):
    """
    Get news for a given date.
    :param date: The date in 'YYYYMMDD' format.
    :param limit: The number of news to fetch.
    :return: A DataFrame containing the news.
    """
    # Connect to the SQLite database
    conn = sqlite3.connect('news_data.db')
    cursor = conn.cursor()
    if limit == 0:
        # Fetch news for the given date
        query = "SELECT * FROM news WHERE id LIKE ?"
        news_df = pd.read_sql_query(query, conn, params=(f"{date}%",))
    else:
        # Fetch random news for the given date
        query = "SELECT * FROM news WHERE id LIKE ? ORDER BY RANDOM() LIMIT ?"
        news_df = pd.read_sql_query(query, conn, params=(f"{date}%", limit))

    # Close the database connection
    conn.close()
    print(news_df['content'])
    return news_df


def get_3_most_similar_historical_news(news_df):
    """
    Get the 3 most similar historical news to the given news.
    :param news_df: A DataFrame containing the news.
    :return: A DataFrame containing the 3 most similar historical news.
    """
    historical_news_happend_dates = []
    corresponding_distances = []
    for index, row in news_df.iterrows():
        content = row['content']
        current_date_time = row['datetime']
        current_date_time = pd.to_datetime(current_date_time)
        current_date_time = current_date_time - pd.Timedelta(days=1)
        print(current_date_time)
        embedding, headers, url = form_header_to_zilliz(content)
        result = search_similar_history_news(embedding, url, headers,current_date_time)
  
        for item in result['data']:
            historical_news_happend_date = item['sqlite_id'][:8]  # Extract the YYYYMMDD part
            historical_news_happend_dates.append(historical_news_happend_date)  # Convert back to string in YYYYMMDD format
            corresponding_distances.append(item['distance'])
    
        data = {
        'historical_news_happend_date': historical_news_happend_dates,
        'corresponding_distance': corresponding_distances
    }
    result_df = pd.DataFrame(data)
    pprint(result_df)
    return result_df


def     prediction(ts_code, historical_news):
    percentage_changes = []
    similarity_scores = []
    for index, row in historical_news.iterrows():
        historical_news_happend_date = row['historical_news_happend_date']
        similarity_score = 1 / row['corresponding_distance']
        next_day_opening_price = get_next_day_opening_price(ts_code, historical_news_happend_date)['open_hfq'].values[0]
        historical_news_happend_date_closing_price = get_closing_price(ts_code, historical_news_happend_date)['close_hfq'].values[0]
        
        # Debugging prints
        print(f"Processing historical news for date: {historical_news_happend_date}")
        print(f"Similarity score: {similarity_score}")
        print(f"Next day opening price: {next_day_opening_price}")
        print(f"Closing price on historical date: {historical_news_happend_date_closing_price}")
        
        percentage_change = (next_day_opening_price - historical_news_happend_date_closing_price) / historical_news_happend_date_closing_price
        percentage_changes.append(percentage_change * similarity_score)
        similarity_scores.append(similarity_score)
    
    weighted_average = sum(percentage_changes) / sum(similarity_scores)
    
    # Debugging prints
    print(f"Percentage changes: {percentage_changes}")
    print(f"Similarity scores: {similarity_scores}")
    print(f"Weighted average percentage change: {weighted_average}")
    
    return weighted_average


if __name__ == '__main__':
    ts_code = '600000.SH'
    print(get_closing_price(ts_code, date='20241003'))
