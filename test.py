from utils.mysql_connect_funcs import delete_table, get_df_tblName

df_metadata = get_df_tblName("metadataTBL")
stocks = df_metadata['symbol'].to_list()
print(stocks[0:5])
for stock in stocks:
    delete_table(f"{stock}_AU_announcements")
