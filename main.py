import tushare as ts
import configparser

config = configparser.ConfigParser()
config.read('dev.ini')
ts_token = config.get('ts', 'token')

pro = ts.pro_api(token=ts_token)
if __name__ == '__main__':

    df = pro.stk_factor(ts_code='000001.SZ', stat_date='20181115', end_date='20241116', fields='close_hfq,trade_date')
    df_news = pro.news(src='sina', start_date='2018-11-21 09:00:00', end_date='2018-11-22 10:10:00')
    df.to_csv("debug.csv")
    print(df_news.head())