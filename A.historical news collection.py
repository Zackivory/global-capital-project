import datetime
import pandas as pd
import tushare as ts
import configparser

config = configparser.ConfigParser()
config.read('dev.ini')
ts_token = config.get('ts', 'token')
pro = ts.pro_api(token=ts_token)


# Define the sources
sources = ['sina', 'wallstreetcn', '10jqka', 'eastmoney', 'yuncaijing', 'fenghuang', 'jinrongjie']

# Define the date range for historical data
start_date = datetime.datetime(2023,11, 15)  # Example start date
end_date = datetime.datetime(2024, 11, 15)  # Example end date

# # Generate a list of trading days
# df = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
# trading_days = df['trade_date'].tolist()

# Collect and store news for all sources for each day in the date range
import os

for day in pd.date_range(start=start_date, end=end_date):
    
    # Define the time range for news collection
    start_time = day.replace(hour=15, minute=0, second=0)
    end_time = day.replace(hour=9, minute=0, second=0)

    # Create folder structure news_data/%Y/%m/
    year_folder = day.strftime('%Y')
    month_folder = day.strftime('%m')
    directory = os.path.join('news_data', year_folder, month_folder)
    os.makedirs(directory, exist_ok=True)
    
    file_name = f"news_all_sources_{day.strftime('%Y%m%d')}.csv"
    file_path = os.path.join(directory, file_name)
    
    # If the file exists, skip
    if os.path.exists(file_path):
        continue

    all_news = pd.DataFrame()
    for src in sources:
        print(src)
        print(start_time.strftime('%Y-%m-%d %H:%M:%S'), (end_time + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))
        df_news = pro.news(src=src, start_date=start_time.strftime('%Y-%m-%d %H:%M:%S'), 
                           end_date=(end_time + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S'))

        df_news['source'] = src
        df_news['content'] = df_news['content'].apply(lambda x: x.replace('\n', ' ') if x is not None else x)
        if src == "yuncaijing":
            df_news['content'] = df_news['content'].apply(lambda x: x.replace('云财经讯，', '') if x is not None else x)
        all_news = pd.concat([all_news, df_news], ignore_index=True)
    # Add an 'id' column to the DataFrame
    all_news['id'] = all_news.index + 1
    # Remove duplicate news
    all_news.drop_duplicates(subset=['title', 'content'], inplace=True)
    
    all_news.to_csv(file_path, index=False)

if __name__ == '__main__':
    print()