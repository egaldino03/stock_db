import investpy
import sqlite3
import pandas as pd

conn = sqlite3.connect('stocks.db')
cursor = conn.cursor()


try:
    # get all stocks from Brazil
    # this returns a pandas DataFrame with 'symbol', 'name', 'full_name', etc.
    stocks_df = investpy.get_stocks(country='brazil')

    # format the tickers for yfinance
    stocks_df['yfinance_ticker'] = stocks_df['symbol'] + '.SA'

    # select only the columns needed for our 'stocks' table
    stocks_to_insert = stocks_df[['yfinance_ticker', 'full_name']]
    stocks_to_insert.rename(columns={
        'yfinance_ticker': 'ticker',
        'full_name': 'company_name'
    }, inplace=True)

    print(f"Found {len(stocks_to_insert)} tickers. Populating database...")

    # insert all tickers into the 'stocks' table
    # convert df to a list of tuples for insertion

    ticker_list_tuples = [tuple(x) for x in stocks_to_insert.to_numpy()]

    cursor.executemany(
        '''
        INSERT OR IGNORE INTO stocks (ticker, company_name)
        VALUES (?, ?)
        ''', ticker_list_tuples
    )

    conn.commit()
    print(f"Successfully added/updated {cursor.rowcount} new tickers to the 'stocks' table.")

except Exception as e:
    print(f"An error occurred: {e}")
    print("This could be a network issue or a change in the 'investpy' library.")

finally:
    conn.close()