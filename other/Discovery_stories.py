import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import write_df_tblName, get_df_tblName
import os
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from io import StringIO
import re


load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = api_key
client = OpenAI()

def run_model(content):
    completion = client.chat.completions.create(
      model="gpt-4o-mini",
      messages=[
        {"role": "system", "content": "you are an expert stock analyst"},
        {"role": "user", "content": content}
      ]
    )
    return completion.choices[0].message.content

df_p = get_df_tblName("Peter_Lynch_Category")
df = df_p[df_p['content'] == 'Turnaround']
tickers = df['ticker'].to_list()

df_h = get_df_tblName("History_short")
df = df_h[df_h['ticker'].isin(tickers)]

current_year = datetime.now().year
last_2_years = {str(year) for year in range(current_year - 1, current_year + 1)}

df = df[df['year'].isin(last_2_years)]
df = df.sample(frac=1, random_state=42).reset_index(drop=True)
df = df.head(50)

df = (
    df
    .assign(content=df['year'].astype(str) + "\n" + df['content'])  # Format year + content
    .groupby('ticker', as_index=False)  # Group by ticker
    .agg({'content': '\n'.join})  # Consolidate content with newline separation
)

tsv_data = df.to_csv(sep='\t', index=False)




prompt = """I have provided you with a TSV below with a 'ticker' column and a 'content' column. 
The table contains stocks which have potential to be turnarounds. 
The 'content' column  contains the recent history of the stock in the corresponding 'ticker' column. 
I would like you to go through the entire table and pick 10 stocks with the most potential of being a turnaround. ie, stocks with the most potential of improving their business operations.
Do not repeat any stocks.
Please return a table with the ticker in one column and the potential turnaround story as dot points in markdown format in another column. 
Make sure the table you return has 2 columns with one of them names ticker.
Be detailed in you reasoning of why each stock was chosen in the potential turnaround story column. 
Do not include any additional text other than the TSV. \n"""

content = prompt + tsv_data
tsv_string = run_model(content)
df_tsv = pd.read_csv(StringIO(tsv_string), sep="|")
df_tsv = df_tsv.loc[:, ~df_tsv.columns.str.contains('^Unnamed')]
df_tsv.columns = df_tsv.columns.str.strip()
df_tsv.columns = df_tsv.columns.str.lower()
df_tsv = df_tsv[~df_tsv['ticker'].str.contains('-')]
df_tsv.columns = ['Ticker', 'Story']
df_tsv['Ticker'] = df_tsv['Ticker'].astype(str).str.strip()
df_tsv['Story'] = df_tsv['Story'].astype(str).str.strip()
df_tsv = df_tsv.dropna(subset=['Ticker'])
df_tsv = df_tsv[df_tsv['Ticker'].str.len() == 3]
df_tsv['Ticker'] = df_tsv['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')
df_tsv = df_tsv.reset_index(drop=True)
for i in range(len(df_tsv)):
    text = df_tsv.at[i,'Story']
    df_tsv.at[i,'Story'] = text.replace(' <br> ', '\n')
    df_tsv.at[i, 'Story'] = text.replace(' <br>', '\n')
    df_tsv.at[i, 'Story'] = text.replace('<br> ', '\n')
    df_tsv.at[i, 'Story'] = text.replace('<br>', '\n')

write_df_tblName('discovery_turnaround', df_tsv)

#cleaned_lines = [line for line in tsv_string.split("\n") if not re.fullmatch(r"\W+", line)]
#cleaned_tsv = "\n".join(cleaned_lines)
#df_tsv = pd.read_csv(StringIO(cleaned_tsv), sep="\t")
#df_tsv.columns = ['Ticker', 'Story']
#



# now for fast growers
df_p = get_df_tblName("Peter_Lynch_Category")
df = df_p[df_p['content'] == 'Fast Grower']
tickers = df['ticker'].to_list()
df = df_h[df_h['ticker'].isin(tickers)]
df = df[df['year'].isin(last_2_years)]
df = df.sample(frac=1, random_state=42).reset_index(drop=True)
df = df.head(50)

df = (
    df
    .assign(content=df['year'].astype(str) + "\n" + df['content'])  # Format year + content
    .groupby('ticker', as_index=False)  # Group by ticker
    .agg({'content': '\n'.join})  # Consolidate content with newline separation
)

tsv_data = df.to_csv(sep='\t', index=False)

prompt = """I have provided you with a TSV below with a 'ticker' column and a 'content' column. 
The table contains stocks which have potential to be fast growers. 
The 'content' column  contains the recent history of the stock in the corresponding 'ticker' column. 
I would like you to go through the entire table and pick 10 stocks with the most potential for growth.
Do not repeat any stocks.
Please return a table  with the ticker in one column and the potential growth story as dot points in markdown format in another column. 
Make sure the table you return has 2 columns with one of them names ticker.
Be detailed in you reasoning of why each stock was chosen in the potential for growth story column. 
Do not include any additional text other than the TSV. \n"""

content = prompt + tsv_data
tsv_string = run_model(content)
df_tsv = pd.read_csv(StringIO(tsv_string), sep="|")
df_tsv = df_tsv.loc[:, ~df_tsv.columns.str.contains('^Unnamed')]
df_tsv.columns = df_tsv.columns.str.strip()
df_tsv.columns = df_tsv.columns.str.lower()
df_tsv = df_tsv[~df_tsv['ticker'].str.contains('-')]
df_tsv.columns = ['Ticker', 'Story']
df_tsv['Ticker'] = df_tsv['Ticker'].astype(str).str.strip()
df_tsv['Story'] = df_tsv['Story'].astype(str).str.strip()
df_tsv = df_tsv.dropna(subset=['Ticker'])
df_tsv = df_tsv[df_tsv['Ticker'].str.len() == 3]
df_tsv['Ticker'] = df_tsv['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')
df_tsv = df_tsv.reset_index(drop=True)
for i in range(len(df_tsv)):
    text = df_tsv.at[i,'Story']
    df_tsv.at[i,'Story'] = text.replace(' <br> ', '\n')
    df_tsv.at[i, 'Story'] = text.replace(' <br>', '\n')
    df_tsv.at[i, 'Story'] = text.replace('<br> ', '\n')
    df_tsv.at[i, 'Story'] = text.replace('<br>', '\n')
write_df_tblName('discovery_growthStory',df_tsv)
