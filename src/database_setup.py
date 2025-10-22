import sqlite3

# connect to the SQlite database (this will create the file)

conn = sqlite3.connect('stocks.db')
cursor = conn.cursor()

# create the 'stocks' table

cursor.execute(
    '''
    CREATE TABLE IF NOT EXISTS stocks (
        id INTEGER PRIMARY KEY,
        ticker TEXT UNIQUE NOT NULL,
        company_name TEXT
    )
    '''
)

cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_prices (
        id INTEGER PRIMARY KEY,
        stock_id INTEGER NOT NULL,
        date TEXT NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume INTEGER NOT NULL,
        FOREIGN KEY (stock_id) REFERENCES stocks (id),
        UNIQUE(stock_id, date) -- Prevents duplicate data for the same day
    )
''')

conn.commit()
conn.close()

print('Database and tables created successfully')