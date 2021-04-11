# program to get historical ticker data as df then convert to Postgresql db
from pandas_datareader.stooq import StooqDailyReader
from sqlalchemy import create_engine


def get_ticker_data(symbols, start, end):
    stooq_data = StooqDailyReader(symbols=symbols, start=start, end=end)
    ticker_data = stooq_data.read()
    return ticker_data


# create engine linked to stockosaurus-db on Postgresql
engine = create_engine('postgresql://dev-admin:123!@localhost:5432/stockosaurus-db')

# get ticker data as df, append ticker and convert to sql
df = get_ticker_data(symbols='TSLA', start='01/01/2020', end='31/12/2020')
df['ticker'] = 'TSLA'#
# df['id'] = [I for I in range(len(df))]
df = df.reset_index()
df.insert(loc=0, column='id', value=df.index)
df.columns = map(str.lower, df.columns)
df = df.set_index('id')
# edit sql table name here
sql = df.to_sql(name='stocks_nasdaqdaily', con=engine, if_exists='append')
