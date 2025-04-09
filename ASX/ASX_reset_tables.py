import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import write_df_tblName
from datetime import datetime
import pytz
import pandas as pd

print("---------- Starting: ASX/ASX_reset_tables.py ----------")

sydney_tz = pytz.timezone("Australia/Sydney")

sydney_time = datetime.now(sydney_tz)

date = sydney_time.strftime('%d %b %Y %I:%M%p')

# todays announcements without price
df = pd.DataFrame([{
    "Date": date,
    "Links": "",
    "Document Name": "START OF DAY",
    "Ticker": "",
    "Type": ""
}])

write_df_tblName('announcements_today', df)
write_df_tblName('announcements_difference', df)

# todays announcements with price
df = pd.DataFrame([{
    "Date": date,
    "Ticker": "",
    "Document Name": "START OF DAY",
    "Type": "",
    "Price Change (%)": ""
}])
write_df_tblName('announcements_today_wPrice', df)

# insider trading today
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
#write_df_tblName('insiderTrades_today', df)

print("---------- Finished: ASX/ASX_reset_tables.py ----------\n\n\n")