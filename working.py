from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName, filter_table, delete_table, get_table_names
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

df = get_df_tblName("metadataTBL")

print(f"Number of stocks: {len(df)}")
l = get_table_names()
#print(l)
IS_ann = [item[0:3] for item in l if "annual_income_statement" in item]
BS_ann = [item[0:3] for item in l if "annual_balance_sheet" in item]
CF_ann = [item[0:3] for item in l if "annual_cash_flow_statement" in item]
IS_qtr = [item[0:3] for item in l if "quarterly_income_statement" in item]
BS_qtr = [item[0:3] for item in l if "quarterly_balance_sheet" in item]
CF_qtr = [item[0:3] for item in l if "quarterly_cash_flow_statement" in item]
print(f"Number of Annual Income Statement: {len(IS_ann)}")
print(f"Number of Annual Balance Sheet: {len(BS_ann)}")
print(f"Number of Annual Cashflow Statement: {len(CF_ann)}")
print(f"Number of Quarterly Income Statement: {len(IS_qtr)}")
print(f"Number of Quarterly Balance Sheet: {len(BS_qtr)}")
print(f"Number of Quarterly Cashflow Statement: {len(CF_qtr)}")


