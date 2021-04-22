# program to get historical ticker data as DataFrame then convert to Postgresql db
import pandas as pd
from pandas_datareader.stooq import StooqDailyReader
from sqlalchemy import create_engine

# select tickers and date range
ticker_list = ['TSLA', 'AMZN', 'AAPL', 'MSFT', 'GOOGL']
start = '01/01/2020'
end = '12/12/2020'


def get_ticker_data(symbols, start, end):
    stooq_data = StooqDailyReader(symbols=symbols, start=start, end=end)
    ticker_data = stooq_data.read()
    return ticker_data


def clean_data(df):
    df = df.reset_index()
    df.insert(loc=0, column='id', value=df.index)
    df.columns = map(str.lower, df.columns)
    df_clean = df.set_index('id')
    return df_clean


def get_all_tickers(tickers, start, end):
    df_all = pd.DataFrame()
    for i in range(len(tickers)):
        df_ticker = get_ticker_data(symbols=tickers[i], start=start, end=end)
        df_ticker['ticker'] = tickers[i]
        df_ticker = clean_data(df_ticker)
        df_ticker['date'] = pd.to_datetime(df_ticker['date']).dt.date
        if i < 1:
            df_all = df_ticker
        else:
            df_all = df_all.append(df_ticker)
    return df_all


df_all = get_all_tickers(ticker_list, start, end)
engine = create_engine('postgresql://postgres:oxford123@localhost:5432/stockosaurus-db')
sql = df_all.to_sql(name='stocks_nasdaqdaily', con=engine, if_exists='append')
