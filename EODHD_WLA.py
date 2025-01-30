from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName


df = get_df_tblName("real_time")
df.loc[:, df.columns != 'code'] = df.loc[:, df.columns != 'code'].astype(float)

losers = df.sort_values(by='change_p', ascending=True)
winners = df.sort_values(by='change_p', ascending=False)
active = df.sort_values(by='volume', ascending=False)

losers = losers.head(10).reset_index(drop=True)
losers = losers[['code', 'close', 'volume', 'change_p']]
losers['code'] = losers['code'].str.replace('.AU', '', regex=False)
losers.columns = ['Ticker', 'Price', 'Volume', 'Change']
losers['Volume'] = losers['Volume'].apply(lambda x: f"{x:,}")
losers['Change'] = losers['Change'].apply(lambda x: f"{x:.2f}%")


winners = winners.head(10).reset_index(drop=True)
winners = winners[['code', 'close', 'volume', 'change_p']]
winners['code'] = winners['code'].str.replace('.AU', '', regex=False)
winners.columns = ['Ticker', 'Price', 'Volume', 'Change']
winners['Volume'] = winners['Volume'].apply(lambda x: f"{x:,}")
winners['Change'] = winners['Change'].apply(lambda x: f"{x:.2f}%")

active = active.head(10).reset_index(drop=True)
active = active[['code', 'close', 'volume', 'change_p']]
active['code'] = active['code'].str.replace('.AU', '', regex=False)
active.columns = ['Ticker', 'Price', 'Volume', 'Change']
active['Volume'] = active['Volume'].apply(lambda x: f"{x:,}")
active['Change'] = active['Change'].apply(lambda x: f"{x:.2f}%")

write_df_tblName("losers",losers)
write_df_tblName("winners",winners)
write_df_tblName("active",active)
