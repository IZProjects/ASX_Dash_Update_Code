from utils.mysql_connect_funcs import get_df_tblName

df_metadata = get_df_tblName("metadataTBL")
stocks = df_metadata['symbol'].to_list()
print(stocks.index('AAC'))