import yfinance as yf
from utils.mysql_connect_funcs import write_df_tblName

# Function to flatten multi-index columns
def flatten_columns(df):
    df.columns = df.columns.get_level_values(0)  # Use only the first level of the MultiIndex
    return df

# The Mains
AXJO = flatten_columns(yf.download('^AXJO', period='1d', interval='15m'))
SPY = flatten_columns(yf.download('SPY', period='1d', interval='15m'))
AUDUSD = flatten_columns(yf.download('AUDUSD=X', period='1d', interval='15m'))

write_df_tblName('AXJO', AXJO)
write_df_tblName('SPY', SPY)
write_df_tblName('AUDUSD', AUDUSD)
