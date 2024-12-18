from pprint import pprint
import random

import pandas as pd

from util import get_random_date, get_list_dates, get_next_day_opening_price, get_current_news, \
    get_3_most_similar_historical_news, get_closing_price, prediction, get_next_day_opening_price_percentage_change,get_stock_list_data
from sklearn.metrics import mean_squared_error, r2_score

if __name__ == '__main__':

    # Example usage

    # Get the list of stock codes from the database
    stock_list = get_stock_list_data()

    # Select 10 random stock codes
    random_stock_codes = random.sample(stock_list, 20)

    result_df = pd.DataFrame(columns=['ts_code', 'random_date', 'next_day_opening_price_percentage_change', 'prediction_result'])
    
    for ts_code in random_stock_codes:
        list_dates = get_list_dates()
        random_date = get_random_date(list_dates)
        print(random_date)
        try:
            next_day_opening_price_percentage_change = get_next_day_opening_price_percentage_change(ts_code, random_date)
        except ValueError:
            continue
        current_news = get_current_news(random_date, limit=20)
        historical_news = get_3_most_similar_historical_news(news_df=current_news)
        try:
            prediction_result = prediction(ts_code, historical_news)
        except ValueError:
            continue
        
        result_df = pd.concat([result_df, pd.DataFrame([{
            'ts_code': ts_code,
            'random_date': random_date,
            'next_day_opening_price_percentage_change': next_day_opening_price_percentage_change,
            'prediction_result': prediction_result
        }])], ignore_index=True)
    
    result_df.to_csv('result.csv', index=False)

    # Assuming 'next_day_opening_price_percentage_change' is the true value and 'prediction_result' is the predicted value
    y_true = result_df['next_day_opening_price_percentage_change']
    y_pred = result_df['prediction_result']

    # Calculate Mean Squared Error
    mse = mean_squared_error(y_true, y_pred)
    print(f"Mean Squared Error: {mse}")

    # Calculate R-squared Score
    r2 = r2_score(y_true, y_pred)
    print(f"R-squared Score: {r2}")