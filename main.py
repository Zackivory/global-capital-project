import tushare as ts
import configparser

config = configparser.ConfigParser()
config.read('dev.ini')
ts_token = config.get('ts', 'token')

pro = ts.pro_api(token=ts_token)
if __name__ == '__main__':

    df = pro.stk_factor(ts_code='600000.SH', start_date='20220501', end_date='20220520', fields='open_hfq')
    df_news = pro.news(src='sina', start_date='2023-11-21 09:00:00', end_date='2023-11-22 10:10:00')
    print(df.head())
    print(df_news.head())