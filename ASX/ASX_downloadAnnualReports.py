import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import requests
import time
import re
from utils.mysql_connect_funcs import get_df_tblName
from utils.OpenAI_functions import run_assistant, get_files, clear_all
import psutil
import sys

from dotenv import load_dotenv
import os

load_dotenv()

proxy_host = os.getenv("proxy_host")
proxy_port = os.getenv("proxy_port")
proxy_user = os.getenv("proxy_user")
proxy_pass = os.getenv("proxy_pass")

proxy = f"{proxy_host}:{proxy_port}"
proxy_auth = f"{proxy_user}:{proxy_pass}"

proxies = {
    'http': f'http://{proxy_auth}@{proxy}',
    'https': f'http://{proxy_auth}@{proxy}'
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
}

def create_folder(folder_path):
  if os.path.exists(folder_path) == False:
    os.mkdir(folder_path)

def sanitize_filename(name):
  return re.sub(r'[<>:"/\\|?*]', '', name)

def download_docs_from_asx(symbol, name, links, dates, parent_path):
  # Ensure the parent directory exists
  os.makedirs(os.path.join(parent_path, symbol), exist_ok=True)

  for i in range(len(links)):
    url = links[i]
    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        response.raise_for_status()
        if response.status_code == 200:
          # Sanitize the name before using it in the file name
          clean_name = sanitize_filename(name[i])
          cleaned_date = sanitize_filename(dates[i])
          file_path = os.path.join(parent_path, symbol, f"{symbol} {cleaned_date} {clean_name}.pdf")
          print(file_path + 'downloaded')

          # Write the file
          with open(file_path, "wb") as f:
            f.write(response.content)
        else:
          print(f"{symbol} {name[i]} Failed to download PDF")
        time.sleep(2)

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")

df = get_df_tblName('announcements_today')
df = df[df['Document Name'].str.contains('annual report', case=False, na=False)]
#df = df[df['Document Name'].str.contains('half yearly', case=False, na=False)]
df = df.reset_index(drop=True)

parent_path = '../annual_reports/'

if not df.empty:
    ticker_dfs = {ticker: group for ticker, group in df.groupby('Ticker')}
    for ticker, ticker_df in ticker_dfs.items():
        names = ticker_df['Document Name'].to_list()
        links = ticker_df['Links'].to_list()
        dates = ticker_df['Date'].to_list()

        folder_path = parent_path + ticker
        create_folder(folder_path)

        download_docs_from_asx(ticker, names, links, dates, parent_path)
        time.sleep(2)
else:
    print("No annual reports")
    sys.exit()


#renaming the files now

def is_file_in_use(filepath):
  for proc in psutil.process_iter():
    try:
      # Check if any process is using the file
      for item in proc.open_files():
        if filepath == item.path:
          return True
    except Exception:
      pass
  return False

def renameFile(message, docs_path, current_path, ticker):
  year_match = re.search(r'\b\d{4}\b', message)
  year = year_match.group()
  new_file_name = docs_path + r'\\' + ticker + r'\\' + ticker + ' ' + year + ' Annual Report.pdf'

  # Retry renaming up to 5 times with a small delay in between
  for attempt in range(5):
    if not is_file_in_use(current_path):
      try:
        os.rename(current_path, new_file_name)
        print(f"Renamed: {current_path} -> {new_file_name}")
        break  # Exit the loop if renaming was successful
      except PermissionError as e:
        print(f"Attempt {attempt + 1}: {e}. Retrying in 1 second...")
        time.sleep(1)  # Wait for 1 second before retrying
    else:
      print(f"File is still in use, waiting for 1 second...")
      time.sleep(1)
  else:
    print(f"Failed to rename {current_path} after multiple attempts.")

prompt = "This is an annual report for a company. What year is it reporting on? Provide only the year in your response.\n"
assistantID = 'asst_itFTGt6IWtVjfqCBrdVMfLdk'
tickers = [folder for folder in os.listdir(parent_path) if os.path.isdir(os.path.join(parent_path, folder))]

for i in range(len(tickers)):
  file_paths, years = get_files(parent_path, tickers[i])
  for j in range(len(file_paths)):
    message = run_assistant(file_paths[j],prompt,assistantID)
    renameFile(message, parent_path, file_paths[j], tickers[i])


try:
  clear_all()
except:
  pass




