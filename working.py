from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName, filter_table, delete_table, get_table_names
import pandas as pd
from datetime import datetime
import pytz

"""sydney_tz = pytz.timezone("Australia/Sydney")

sydney_time = datetime.now(sydney_tz)

date = sydney_time.strftime('%d %b %Y %I:%M%p')

df = pd.DataFrame([{
    "Date": date,
    "Ticker": "",
    "Director": "",
    "Type": "",
    "Number of Shares": "",
    "Price per Share": "",
    "Value": "",
    "Notes": "START OF DAY",

}])
write_df_tblName('insiderTrades_total', df)"""

df = get_df_tblName("insiderTrades_total")
print(df.at[0,'Date'])
print(type(df.at[0,'Date']))

df = get_df_tblName("insiderTrades_today")
print(df.at[0,'Date'])
print(type(df.at[0,'Date']))