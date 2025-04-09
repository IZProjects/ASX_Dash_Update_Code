import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import pandas as pd
from datetime import datetime, timedelta
import pytz
from utils.EODHD_functions import get_weekly_data, get_monthly_data, get_historical_stock_data
from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName
import logging

print("---------- Starting: EODHD/EODHD_update_EOD.py ----------")

# Configure logging
logging.basicConfig(
    filename="EODHD_update_EOD.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Suppress pandas SettingWithCopyWarning
pd.options.mode.chained_assignment = None


# Load stock tickers
try:
    df_stocks = get_df_tblName("ticker_list")
    stocks = df_stocks['tickers'].to_list()
    stocks = [item + ".AU" for item in stocks]
except Exception as e:
    logging.error(f"Error loading stock tickers: {e}", exc_info=True)
    raise

# Define date range
from_date = "2000-01-01"
to_date = datetime.today().strftime('%Y-%m-%d')

# Get current date and time in Australian Sydney Time
aus_tz = pytz.timezone('Australia/Sydney')
current_time = datetime.now(aus_tz)

# Check if today is the end of the week (Friday)
is_end_of_week = current_time.weekday() == 4  # 4 represents Friday

# Check if today is the end of the month
next_day = current_time + timedelta(days=1)
is_end_of_month = current_time.month != next_day.month

for stock in stocks:
    try:
        # Fetch historical stock data
        stock_data = get_historical_stock_data(stock, from_date, to_date)
        stockName = stock.replace(".", "_")

        # Daily data processing
        df_daily = stock_data[-260:] if len(stock_data) > 260 else stock_data
        df_daily = df_daily.reset_index(drop=True)
        daily_name = (stockName + '_daily').replace('.', '_')
        write_df_tblName(daily_name, df_daily)

        # Weekly data processing (only at the end of the week)
        if is_end_of_week:
            df_weekly = get_weekly_data(stock_data)
            df_weekly = df_weekly[-52 * 5:] if len(df_weekly) > 52 * 5 else df_weekly
            df_weekly = df_weekly.reset_index(drop=True)
            weekly_name = (stockName + '_weekly').replace('.', '_')
            write_df_tblName(weekly_name, df_weekly)

        # Monthly data processing (only at the end of the month)
        if is_end_of_month:
            df_monthly = get_monthly_data(stock_data)
            df_monthly = df_monthly.reset_index(drop=True)
            monthly_name = (stockName + '_monthly').replace('.', '_')
            write_df_tblName(monthly_name, df_monthly)

    except Exception as e:
        print(f"{stock} failed with error: {e}")
        logging.error(f"{stock} failed with error: {e}", exc_info=True)

logging.info("run complete")
print("---------- Finished: EODHD/EODHD_update_EOD.py ----------\n\n\n")