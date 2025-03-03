from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName, filter_table, delete_table
from utils.ASX_functions import getAnnouncements
import os
from utils.mysql_connect_funcs import write_df_tblName
from datetime import datetime
import pytz
import pandas as pd
#delete_table("insiderTrades_today")
from dotenv import load_dotenv
from openai import OpenAI
from io import StringIO
import re

#print(get_df_tblName("announcements_today").at[0,'Document Name'])

df = get_df_tblName("MMR_AU_daily")
print(df)