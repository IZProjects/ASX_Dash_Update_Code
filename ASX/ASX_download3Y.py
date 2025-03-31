import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import get_df_tblName, insert_row_FR
from utils.OpenAI_functions import run_assistant
import pandas as pd
import os
import requests
import time
import re
from io import StringIO
from datetime import datetime
import pytz


def format_dollars(value):
  value = str(value)
  value = re.sub(r"[^\d.]", "", value)
  if value != '':
    num = float(value)
    return f"${num:,.2f}"
  else:
    return "NA"


def sanitize_filename(name):
  return re.sub(r'[<>:"/\\|?*]', '', name)

def download_docs_from_asx(symbol, name, links, dates, parent_path):
  os.makedirs(os.path.join(parent_path, symbol), exist_ok=True)

  for i in range(len(links)):
    url = links[i]
    response = requests.get(url)

    if response.status_code == 200:
      clean_name = sanitize_filename(name[i])
      cleaned_date = sanitize_filename(dates[i])
      file_path = os.path.join(parent_path, symbol, f"{symbol} {cleaned_date} {clean_name}.pdf")
      print(file_path + 'downloaded')

      with open(file_path, "wb") as f:
        f.write(response.content)
    else:
      print(f"{symbol} {name[i]} Failed to download PDF")

    time.sleep(2)


df = get_df_tblName("announcements_difference")

filter_values = ["appendix 3y", "change of director's interest"]


df = df[
    df["Document Name"].str.contains("|".join(filter_values), case=False, na=False) |
    df["Type"].str.contains("|".join(filter_values), case=False, na=False)
]

df = df.reset_index(drop=True)
path = '../Appendix_3Y/'

if not df.empty:

  for i in range(len(df)):
    url = df.at[i,'Links']
    response = requests.get(url)
    symbol = df.at[i, 'Ticker']
    if response.status_code == 200:
      # Sanitize the name before using it in the file name
      clean_name = sanitize_filename(df.at[i,"Document Name"])
      cleaned_date = sanitize_filename(df.at[i,"Date"])
      file_path = os.path.join(path, f"{i} {symbol} {cleaned_date} {clean_name}.pdf")
      print(file_path + ' downloaded')
      with open(file_path, "wb") as f:
        f.write(response.content)
    else:
      print(f"{symbol} Appendix_3Y Failed to download PDF")
    time.sleep(2)

  file_paths = []
  for root, _, files in os.walk(path):
      for file in files:
          file_paths.append(os.path.join(root, file))
  file_paths = sorted(file_paths)


  sydney_tz = pytz.timezone("Australia/Sydney")
  sydney_time = datetime.now(sydney_tz)
  date = sydney_time.strftime("%d %b %Y")

  assistantID = "asst_itFTGt6IWtVjfqCBrdVMfLdk"
  prompt = """
  The file attached a form submitted to the ASX about a director's change in stock holdings. For each transaction, can you please extract: The name of the director, whether stock was bought, sold or issued, the number of shares transacted, the price per share, the total value of the transaction and the type of transaction it was (eg. if it was issued or an on-market transaction, etc). 
  Give the information back to me in TSV format with the 6 columns as: Director, Bought/Sold/Issued, Number of Shares, Price per Share, Value, Type.
  Include the column names in the TSV and make sure it has the 6 columns above exactly. Do not return anything other than the TSV.
  """

  dfs = []
  for i in range(len(file_paths)):
    tsv_string = run_assistant(file_paths[i],prompt,assistantID)
    cleaned_lines = [line for line in tsv_string.split("\n") if not re.fullmatch(r"\W+", line)]
    cleaned_tsv = "\n".join(cleaned_lines)
    df_tsv = pd.read_csv(StringIO(cleaned_tsv), sep="\t")
    #df_tsv = df_tsv.dropna()
    if not df_tsv.empty and df_tsv.shape[1] == 6:
      df_tsv.columns = ['Director', 'Bought/Sold/Issued', 'Number of Shares', 'Price per Share', 'Value', 'Type']
      tickers = [df.at[i,'Ticker'] for _ in range(len(df_tsv))]
      dates = [df.at[i, 'Date'] for _ in range(len(df_tsv))]
      new_cols = pd.DataFrame({"Date": dates, "Ticker": tickers})
      df_tsv = pd.concat([new_cols, df_tsv], axis=1)
      dfs.append(df_tsv)


  combined_df = pd.concat(dfs, ignore_index=True)
  combined_df['Price per Share'] = combined_df['Price per Share'].apply(format_dollars)
  combined_df['Value'] = combined_df['Value'].apply(format_dollars)
  combined_df.columns = ['Date', 'Ticker', 'Director', 'Type', 'Number of Shares', 'Price per Share', 'Value', 'Notes']
  combined_df['Ticker'] = combined_df['Ticker'].apply(lambda x: f"[{x}](/02-companyoverview?data={x}_AU)")

  for i in range(len(combined_df)):
      row = combined_df.iloc[i].tolist()
      insert_row_FR(f"insiderTrades_today", row, ['Date', 'Ticker', 'Director', 'Type', 'Number of Shares', 'Price per Share', 'Value', 'Notes'])

  combined_df.to_csv("insider.csv", index=False)

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    if os.path.isfile(file_path):
      os.remove(file_path)
      print(f"Deleted {filename}")

else:
  print("empty")