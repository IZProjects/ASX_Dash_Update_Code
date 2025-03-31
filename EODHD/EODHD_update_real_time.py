import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import pandas as pd
from utils.EODHD_functions import get_real_time_multi_stock_data
from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName
import logging

# Configure logging
logging.basicConfig(
    filename="EODHD_update_real_time.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
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
            logging.warning(f"Unexpected data format received: {data}")

    # Flatten the rows
    rows_flattened = []
    for sublist in rows:
        if all(isinstance(item, dict) for item in sublist):  # Ensure sublist contains dictionaries
            rows_flattened.extend(sublist)
        else:
            logging.warning(f"Unexpected sublist format: {sublist}")

    # Convert to DataFrame
    df = pd.DataFrame(rows_flattened)

    # Clean the DataFrame
    df = df.replace('NA', pd.NA).dropna(how='any')

    # Write to database
    write_df_tblName("real_time", df)

    logging.info("Data successfully written to the database.")
    print("Data successfully written to the database.")

except Exception as e:
    logging.error(f"An error has occurred: {e}", exc_info=True)
    print(f"An error has occurred: {e}")

