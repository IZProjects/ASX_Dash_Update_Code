from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName, filter_table, delete_table, get_table_names
import pandas as pd
from datetime import datetime
import pytz

delete_table("insiderTrades_today")

