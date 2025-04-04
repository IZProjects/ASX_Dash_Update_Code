import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName
import os
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from io import StringIO

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = api_key
client = OpenAI()

#docs_path = "../annual_reports/"
tickers = [folder for folder in os.listdir(os.path.join(parent_path, "annual_reports")) if os.path.isdir(os.path.join(parent_path, "annual_reports",folder))]

if not tickers:
    print('No updates')
else:
    df = get_df_tblName("segmentResultsExtract")
    for i in range(len(tickers)):
        try:
            filtered_df = df[df['ticker'] == tickers[i]]
            combined_contents = filtered_df['content'].str.cat(sep='\n')

            content = "You have been given the some business segment information of a company for multiple years. Please extract out the operating results for each year and create a pivot table. Have the financial metric as the first column and the business segments as the other columns. Ensure you include a year column. Fill in anything missing with N/A. Do not add any explanatory text, notes or headings outside the table created.:\n" + combined_contents

            completion = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system",
                     "content": "You are an editing assistant. Your job is to create summary tables. Only create the tables. Do not write anthing outside the table."},
                    {"role": "user", "content": content}
                ]
            )

            data = completion.choices[0].message.content
            df2 = pd.read_csv(StringIO(data), sep="|")
            df2 = df2.loc[:, ~df2.columns.str.contains('^Unnamed')]
            df2.columns = df2.columns.str.strip()
            df2.columns = df2.columns.str.lower()
            if 'year' in df2.columns:
                df2 = df2[~df2['year'].str.contains('-')]

            print(df2)
            write_df_tblName(tickers[i] + '_segmentResults',df2)
            print(tickers[i] + " success")
        except Exception as e:
            print(tickers[i] + f' failed. An error has occurred: {e}')
