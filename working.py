from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName, filter_table, delete_table, get_table_names
import pandas as pd
from datetime import datetime
import pytz

sydney_tz = pytz.timezone("Australia/Sydney")

sydney_time = datetime.now(sydney_tz)

date = sydney_time.strftime('%d %b %Y %I:%M%p')

df = pd.DataFrame([{
    "Date": date,
    "Ticker": "",
    "Document Name": "START OF DAY",
    "Type": "",
    "Document Number": 0000,
    "Price Change (%)": ""
}])
write_df_tblName('announcements_today_wPrice', df)