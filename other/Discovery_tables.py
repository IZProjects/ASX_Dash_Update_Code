import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName

import pandas as pd

print("---------- Starting: other/Discovery_tables.py ----------")

pd.options.mode.chained_assignment = None  # default='warn'
def format_number(num):
    if num >= 1_000_000_000:
        formatted_number = f"{num / 1_000_000_000:.1f} B"
    elif num >= 1_000_000:
        formatted_number = f"{num / 1_000_000:.1f} M"
    elif num >= 1_000:
        formatted_number = f"{num / 1_000:.1f} K"
    else:
        formatted_number = f"{num:.1f}"  # Keep as-is for smaller numbers

    return formatted_number

def process_deep_value(df):
    df = df.sort_values(by='Price_to_Free_Cash_Flow')
    df = df.head(100)
    df = df.sample(n=min(10, len(df)))
    df = df.sort_values(by='Price_to_Free_Cash_Flow').reset_index(drop=True)
    df = df[['Item', 'Price_to_Free_Cash_Flow', 'Debt_to_Equity', 'EBITDA_Margin', 'Market_Cap']]
    df['EBITDA_Margin'] = (df['EBITDA_Margin']*100).map('{:.2f}%'.format)
    df['Market_Cap'] = df['Market_Cap'].apply(format_number)
    df['Price_to_Free_Cash_Flow'] = df['Price_to_Free_Cash_Flow'].round(2)
    df['Debt_to_Equity'] = df['Debt_to_Equity'].round(2)
    df['Item'] = df['Item'].apply(lambda x: f'[{x[0:3]}](/02-companyoverview?data={x})')
    df.columns = ['Ticker', 'P/FCF', 'D/E', 'EBITDA Margin', 'Market Cap']
    return df

def process_ROE(df):
    df = df.sort_values(by='Return_on_Equity', ascending=False)
    df = df.head(100)
    df = df.sample(n=min(10, len(df)))
    df = df.sort_values(by='Return_on_Equity', ascending=False).reset_index(drop=True)
    df = df[['Item', 'Return_on_Equity', 'Debt_to_Equity', 'EBITDA_Margin', 'Market_Cap']]
    df['EBITDA_Margin'] = (df['EBITDA_Margin']*100).map('{:.2f}%'.format)
    df['Return_on_Equity'] = (df['Return_on_Equity']*100).map('{:.2f}%'.format)
    df['Market_Cap'] = df['Market_Cap'].apply(format_number)
    df['Debt_to_Equity'] = df['Debt_to_Equity'].round(2)
    df['Item'] = df['Item'].apply(lambda x: f'[{x[0:3]}](/02-companyoverview?data={x})')
    df.columns = ['Ticker', 'ROE', 'D/E', 'EBITDA Margin', 'Market Cap']
    return df

def process_growth(df):
    df['PEG'] = df['Price_to_Earnings'] / df['Net_Income_Growth']
    df = df[(df['PEG'] < 1.5)]
    df = df.head(100)
    df = df.sample(n=min(10, len(df)))
    df = df.sort_values(by='Revenue_Growth', ascending=False).reset_index(drop=True)
    df = df[['Item', 'Revenue_Growth', 'PEG', 'Market_Cap']]
    df['Market_Cap'] = df['Market_Cap'].apply(format_number)
    df['Revenue_Growth'] = (df['Revenue_Growth']*100).map('{:.2f}%'.format)
    df['PEG'] = df['PEG'].round(2)
    df['Item'] = df['Item'].apply(lambda x: f'[{x[0:3]}](/02-companyoverview?data={x})')
    df.columns = ['Ticker', 'Revenue Growth', 'PEG',  'Market Cap']
    return df

df = get_df_tblName("Screener_TBL2")

df['Price_to_Free_Cash_Flow'] = pd.to_numeric(df['Price_to_Free_Cash_Flow'], errors='coerce')
df['Debt_to_Equity'] = pd.to_numeric(df['Debt_to_Equity'], errors='coerce')
df['Price_to_Earnings'] = pd.to_numeric(df['Price_to_Earnings'], errors='coerce')
df['EBITDA_Margin'] = pd.to_numeric(df['EBITDA_Margin'], errors='coerce')
df['Return_on_Equity'] = pd.to_numeric(df['Return_on_Equity'], errors='coerce')
df['Revenue_Growth'] = pd.to_numeric(df['Revenue_Growth'], errors='coerce')
df['Net_Income_Growth'] = pd.to_numeric(df['Net_Income_Growth'], errors='coerce')
df['Market_Cap'] = pd.to_numeric(df['Market_Cap'], errors='coerce')



df1 = df[(df['Price_to_Free_Cash_Flow'] > 0) & (df['Price_to_Free_Cash_Flow'] < 15) & (df['Debt_to_Equity'] < 1) & (df['Debt_to_Equity'] > 0) & (df['EBITDA_Margin'] > 0)]
write_df_tblName("discovery_deep_value", process_deep_value(df1))

df1 = df1[(df1['Market_Cap'] > 100000000)]
write_df_tblName("discovery_deep_value_100M", process_deep_value(df1))

df2 = df[(df['Return_on_Equity'] > 0.2) & (df['Debt_to_Equity'] < 1) & (df['Debt_to_Equity'] > 0) & (df['EBITDA_Margin'] > 0)]
write_df_tblName("discovery_ROE", process_ROE(df2))

df2 = df2[(df2['Market_Cap'] > 100000000)]
write_df_tblName("discovery_ROE_100M", process_ROE(df2))

df3 = df[(df['Revenue_Growth'] > 0.2) & (df['Price_to_Earnings'] > 0) & (df['Net_Income_Growth'] > 0)]
write_df_tblName("discovery_growth", process_growth(df3))

df3 = df3[(df3['Market_Cap'] > 100000000)]
write_df_tblName("discovery_growth_100M", process_growth(df3))

print("---------- Finished: other/Discovery_tables.py ----------\n\n\n")