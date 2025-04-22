import pandas as pd
from quickFS_functions import get_metadata
from mysql_connect_funcs import get_df_tblName, write_df_tblName
pd.options.mode.chained_assignment = None  # default='warn'


#df_existing = get_df_tblName("metadataTBL")

df_stocks = pd.read_csv(r'C:\00 App Projects\ASX Dashboard\App 1\databases\ticker_list.csv')
stocks = df_stocks['tickers'].to_list()
stocks = [item + ":AU" for item in stocks]
#note total number of stocks is 1922
#stocks = stocks[1500:len(stocks)]

dfs = []
for i in range(len(stocks)):
  df = get_metadata(stocks[i])
  dfs.append(df)
  print(i)

metadata = pd.concat(dfs, ignore_index=True)

#df_final = pd.concat([df_existing,metadata], ignore_index=True)
df_final = metadata.drop_duplicates()
print(df_final.head())
print(len(df_final))
print(df_final.columns)
write_df_tblName("metadataTBL",df_final)



