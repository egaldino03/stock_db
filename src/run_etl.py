import sys
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import sqlite3

# 1. SETUP: Create the database engine
db_engine = create_engine('sqlite:///stocks.db')

# 2. EXTRACT: get all tickers from our database
conn = sqlite3.connect('stocks.db')
# SELECT id AS stock_id so the merged dataframe will have 'stock_id'
stocks_df = pd.read_sql('SELECT id AS stock_id, ticker FROM stocks', conn)
conn.close()

# --- FIX 1: Normalize case for the database tickers ---
# Force all tickers from the DB to be uppercase for a reliable merge
stocks_df['ticker'] = stocks_df['ticker'].str.upper()

ticker_list = stocks_df['ticker'].tolist()
if not ticker_list:
    print('No tickers found in the database. Exiting.')
    sys.exit(1)

print(f'fetching data for all {len(ticker_list)} tickers...')

# Download all historical data for all tickers
try:
    data = yf.download(tickers=ticker_list, start='2024-01-01')
except Exception as e:
    print(f'Error fetching data from yfinance: {e}')
    sys.exit(1)

if data.empty:
    print('No data downloaded. Tickers may be invalid or yfinance API is down. Exiting.')
    sys.exit(1)

# Handle the case where yfinance returns a single-ticker DataFrame (no MultiIndex columns)
if not isinstance(data.columns, pd.MultiIndex):
    # make columns a MultiIndex with levels (feature, ticker)
    # data.columns are features; attach the single ticker as the second level
    single_ticker = ticker_list[0]
    data.columns = pd.MultiIndex.from_product([data.columns, [single_ticker]])

print('Download complete. Transforming data...')

# --- START OF TRANSFORM ---

# 1. Name the column levels explicitly
data.columns.names = ['feature', 'ticker']

# 2. Stack the 'ticker' level into the index
df = data.stack(level='ticker')

# 3. Name the index levels before resetting
df.index.names = ['date', 'ticker']

# 4. Reset the index
df = df.reset_index()

# --- FIX 3: Normalize case for the downloaded yfinance tickers ---
# Force all tickers from yfinance to be uppercase to match the DB
df['ticker'] = df['ticker'].str.upper()

# 5. Rename the feature columns (drop/ignore 'Adj Close' if present)
rename_map = {
    'Open': 'open',
    'High': 'high',
    'Low': 'low',
    'Close': 'close',
    'Volume': 'volume',
    'Adj Close': 'adj_close'
}
df = df.rename(columns=rename_map)

# Keep only the columns we care about (ignore adj_close)
expected_cols = ['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']
missing = [c for c in expected_cols if c not in df.columns]
if missing:
    print(f'Warning: expected columns missing after transform: {missing}')
    # continue — missing columns will cause later errors if critical

df = df[[c for c in expected_cols if c in df.columns]]

# Swap the ticker string for the stock_id integer
# This merge will work because both 'ticker' columns are uppercase
df = pd.merge(df, stocks_df, on='ticker', how='left')

# --- FIX 4: Add a check to see if the merge failed ---
if df['stock_id'].isna().all():
    print("Merge failed. No matching tickers were found between yfinance and database.")
    print("This is a critical error. Exiting.")
    sys.exit(1)

# Drop rows without a stock_id (no matching ticker)
df = df.dropna(subset=['stock_id'])

# Ensure stock_id is integer
df['stock_id'] = df['stock_id'].astype(int)

# Reorder/select final columns
df = df[['stock_id', 'date', 'open', 'high', 'low', 'close', 'volume']]

# Convert date to string to avoid timezone issues in SQLite
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

print(f'transformed {len(df)} rows of data')

# 4. LOAD: send the data to the database
try:
    df.to_sql(
        'daily_prices',
        con=db_engine,
        if_exists='append', # appends new data
        index=False,
        chunksize=1000 # Loads data in batches
    )
    print('Successfully loaded data into daily_prices.')

except Exception as e:
    print(f'Error loading data: {e}')
    print("This may be due to the UNIQUE constraint (stock_id, date).")
    print("This is normal if data for today already exists.")
    sys.exit(1)