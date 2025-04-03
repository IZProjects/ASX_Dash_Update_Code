import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.ASX_functions import getAnnouncements
from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName, filter_table
from datetime import datetime
import pytz
import pandas as pd


sydney_tz = pytz.timezone("Australia/Sydney")
sydney_time = datetime.now(sydney_tz)
date = sydney_time.strftime("%d %b %Y")

url = 'https://www2.asx.com.au/markets/trade-our-cash-market/announcements'

#df_existing = getAnnouncements(url, 1)
df_existing = get_df_tblName('announcements_today')
df_metadata = get_df_tblName("metadataTBL")

df_new = getAnnouncements(url, 3)
df_new.columns = [col.title() for col in df_new.columns]

df_new = df_new[df_new["Date"].str.contains(date, na=False)]
df_new = df_new[df_new["Ticker"].isin(df_metadata["symbol"])]

df_difference = df_new.merge(df_existing, how="left", indicator=True).query('_merge == "left_only"').drop(columns=["_merge"])
df_difference = df_difference.reset_index(drop=True)
write_df_tblName('announcements_difference', df_difference)

df_combined = pd.concat([df_existing, df_difference]).reset_index(drop=True)


write_df_tblName('announcements_today',df_combined)

tickers = df_combined['Ticker'].to_list()
tickers = [ticker + ".AU" for ticker in tickers]
df_prices = filter_table('real_time', 'code', tickers)
df_prices['code'] = df_prices['code'].str.replace('.AU', '', regex=False)
df_prices['change_p'] = df_prices['change_p'].apply(lambda x: x[:-2] if len(x) > 5 else x)
df_combined['Price Change (%)'] = df_combined['Ticker'].map(df_prices.set_index('code')['change_p'])
df_combined = df_combined.dropna().drop_duplicates().reset_index(drop=True)
df_combined['Document Name'] = df_combined['Document Name'].str.upper()
df_combined['Type'] = df_combined['Type'].str.upper()
df_combined['Price Change (%)'] = df_combined['Price Change (%)'].astype(float)
df_combined['Ticker'] = df_combined['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')
df_combined["Document Name"] = df_combined.apply(lambda row: f'[{row["Document Name"]}]({row["Links"]})', axis=1)
df_combined = df_combined.drop(columns=["Links"])
cols = ["Date", "Ticker"] + [col for col in df_combined.columns if col not in ["Date", "Ticker"]]
df_combined = df_combined[cols]
write_df_tblName("announcements_today_wPrice", df_combined)
print(df_combined)

"""for i in range(len(df_difference)):
    ticker = df_difference.at[i, 'Ticker']
    row = df_difference.iloc[i].tolist()
    insert_row_FR(f"{ticker}_AU_announcements", row, ['Date', 'Links', 'Document Name', 'Ticker', 'Type'])
"""

