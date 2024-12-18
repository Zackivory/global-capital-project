import datetime
import sqlite3

import pandas as pd
import tushare as ts
import configparser

def save_stock_list_data_to_db():
    config = configparser.ConfigParser()
    config.read('dev.ini')
    ts_token = config.get('ts', 'token')
    pro = ts.pro_api(token=ts_token)

    data = pro.stock_basic(exchange='SSE,SZSE', list_status='L', market="主板", fields='ts_code,symbol,name,area,industry,list_date')

    # Connect to SQLite database
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()

    # Check if the table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stock_data'")
    table_exists = cursor.fetchone()

    if not table_exists:
        # Save the dataframe to the SQLite database
        data.to_sql('stock_list_data', conn, if_exists='replace', index=False)
        print("Data saved to 'stock_list_data' table in 'stock_data.db'.")
    else:
        print("Table 'stock_list_data' already exists. Skipping data save.")

    # Close the database connection
    conn.close()
def initialize_historical_stock_price_table():
    # Connect to SQLite database
    conn = sqlite3.connect('stock_data.db')
    cursor = conn.cursor()

    # Create the table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS historical_stock_price_data (
            ts_code TEXT,
            trade_date TEXT,
            open_hfq REAL,
            close_hfq REAL
        )
    ''')
    print("Table 'historical_stock_price_data' initialized in 'stock_data.db'.")

    # Close the database connection
    conn.close()

def save_historical_stock_price_data_to_db(ts_code, start_date, end_date):
    config = configparser.ConfigParser()
    config.read('dev.ini')
    # Connect to SQLite database
    conn = sqlite3.connect('stock_data.db')
    ts_token = config.get('ts', 'token')
    pro = ts.pro_api(token=ts_token)        
    df = pro.stk_factor(ts_code=ts_code, start_date=start_date, end_date=end_date, fields='ts_code,trade_date,open_hfq, close_hfq')
    df.to_sql('historical_stock_price_data', conn, if_exists='append', index=False)


    
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



if __name__ == '__main__':
    save_stock_list_data_to_db()
    initialize_historical_stock_price_table()
    for ts_code in get_stock_list_data():
        print(ts_code)
        save_historical_stock_price_data_to_db(ts_code, '20181115', '20241116')
