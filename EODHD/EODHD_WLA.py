import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName
import logging
import pandas as pd

# Configure logging
logging.basicConfig(
    filename="EODHD_WLA.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_GLA(df,loserName, winnerName, activeName):
    try:
        df.loc[:, df.columns != 'code'] = df.loc[:, df.columns != 'code'].astype(float)

        losers = df.sort_values(by='change_p', ascending=True)
        winners = df.sort_values(by='change_p', ascending=False)
        active = df.sort_values(by='volume', ascending=False)

        losers = losers.head(20).reset_index(drop=True)
        losers = losers[['code', 'close', 'volume', 'change_p']]
        losers['code'] = losers['code'].str.replace('.AU', '', regex=False)
        losers.columns = ['Ticker', 'Price', 'Volume', 'Change']
        losers['Volume'] = losers['Volume'].apply(lambda x: f"{x:,}")
        losers['Price'] = losers['Price'].apply(lambda x: f"${x}")
        losers['Change'] = losers['Change'].apply(lambda x: f"{x:.2f}%")
        losers['Ticker'] = losers['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')


        winners = winners.head(20).reset_index(drop=True)
        winners = winners[['code', 'close', 'volume', 'change_p']]
        winners['code'] = winners['code'].str.replace('.AU', '', regex=False)
        winners.columns = ['Ticker', 'Price', 'Volume', 'Change']
        winners['Volume'] = winners['Volume'].apply(lambda x: f"{x:,}")
        winners['Price'] = winners['Price'].apply(lambda x: f"${x}")
        winners['Change'] = winners['Change'].apply(lambda x: f"{x:.2f}%")
        winners['Ticker'] = winners['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')

        active = active.head(20).reset_index(drop=True)
        active = active[['code', 'close', 'volume', 'change_p']]
        active['code'] = active['code'].str.replace('.AU', '', regex=False)
        active.columns = ['Ticker', 'Price', 'Volume', 'Change']
        active['Volume'] = active['Volume'].apply(lambda x: f"{x:,}")
        active['Price'] = active['Price'].apply(lambda x: f"${x}")
        active['Change'] = active['Change'].apply(lambda x: f"{x:.2f}%")
        active['Ticker'] = active['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')

        write_df_tblName(loserName,losers)
        write_df_tblName(winnerName,winners)
        write_df_tblName(activeName,active)

        logging.info("Data successfully written to the database.")
        print("Data successfully written to the database.")

    except Exception as e:
        logging.error(f"An error has occurred: {e}", exc_info=True)
        print(f"An error has occurred: {e}")

df = get_df_tblName("real_time")
get_GLA(df,"losers","winners","active")

df_screener = get_df_tblName("Screener_TBL2")
df_screener['Market_Cap'] = pd.to_numeric(df_screener['Market_Cap'], errors='coerce')
df_screener = df_screener[df_screener['Market_Cap'] > 100000000]
tickers = df_screener['Item'].to_list()
tickers = [s.replace("_", ".") for s in tickers]
df_100M = df[df['code'].isin(tickers)]
get_GLA(df_100M,"losers100M","winners100M","active100M")

