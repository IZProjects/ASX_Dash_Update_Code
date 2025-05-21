from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName
import pandas as pd
#df = get_df_tblName("insiderTrades_total")
df = get_df_tblName("insiderTrades_today")
print(df)

