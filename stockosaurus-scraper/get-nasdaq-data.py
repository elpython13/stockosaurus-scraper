# program to get historical ticker data as df then convert to Postgresql db
from pandas_datareader.stooq import StooqDailyReader
from sqlalchemy import create_engine


def get_ticker_data(symbols, start, end):
    stooq_data = StooqDailyReader(symbols=symbols, start=start, end=end)
    ticker_data = stooq_data.read()
    return ticker_data


# create engine linked to stockosaurus-db on Postgresql
engine = create_engine('postgresql://dev-admin:123!@localhost:5432/stockosaurus-db')

# get ticker data as df and sql (edit ticker and sql table name here)
df = get_ticker_data(symbols='TSLA', start='01/01/2020', end='31/12/2020')
sql = df.to_sql(name='TSLA_SQL_2020', con=engine)
