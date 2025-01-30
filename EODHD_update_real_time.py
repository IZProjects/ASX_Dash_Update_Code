import pandas as pd
import sqlite3
from utils.EODHD_functions import get_real_time_multi_stock_data
from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName

# Load the list of stocks
df_stocks = get_df_tblName("ticker_list")
stocks = df_stocks['tickers'].to_list()
stocks = [item + ".AU" for item in stocks]

# Chunk the stocks list
chunk_size = 20
stocks_chunked = [stocks[i:i + chunk_size] for i in range(0, len(stocks), chunk_size)]

# Retrieve and process data
rows = []
for chunk in stocks_chunked:
    data = get_real_time_multi_stock_data(chunk)
    if isinstance(data, list):  # Ensure data is a list
        rows.append(data)
    else:
        print(f"Unexpected data format: {data}")  # Debugging aid

# Flatten the rows
rows_flattened = []
for sublist in rows:
    if all(isinstance(item, dict) for item in sublist):  # Ensure sublist contains dictionaries
        rows_flattened.extend(sublist)
    else:
        print(f"Unexpected sublist format: {sublist}")  # Debugging aid

# Convert to DataFrame
df = pd.DataFrame(rows_flattened)

# Clean the DataFrame
df = df.replace('NA', pd.NA).dropna(how='any')

# Write to database
write_df_tblName("real_time", df)
