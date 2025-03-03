import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from datetime import datetime

from utils.EODHD_functions import get_weekly_data, get_monthly_data, get_historical_stock_data
from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName

pd.options.mode.chained_assignment = None  # default='warn'

#path = 'mass_upload/difference.csv'
path = 'difference.csv'
df_stocks = pd.read_csv(path)
stocks = df_stocks['tickers'].to_list()
stocks = [item + ".AU" for item in stocks]

#indiv prices

from_date = "2000-01-01"
to_date = datetime.today().strftime('%Y-%m-%d')

for stock in stocks:
  try:
    stock_data = get_historical_stock_data(stock, from_date, to_date)
    stockName = stock.replace(".", "_")

    df_daily = stock_data[-260:] if len(stock_data) > 260 else stock_data
    df_daily = df_daily.reset_index(drop=True)
    name = (stockName + '_daily').replace('.', '_')
    write_df_tblName(name, df_daily)

    df = get_weekly_data(stock_data)
    df_weekly = df[-52*5:] if len(df) > 52*5 else df
    df_weekly = df_weekly.reset_index(drop=True)
    name = (stockName + '_weekly').replace('.', '_')
    write_df_tblName(name, df_weekly)

    df_monthly = get_monthly_data(stock_data)
    df_monthly = df_monthly.reset_index(drop=True)
    name = (stockName + '_monthly').replace('.', '_')
    write_df_tblName(name, df_monthly)

  except Exception as e:
    print(f'{stock} failed: {e}')

