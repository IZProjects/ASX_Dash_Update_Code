import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import time
import re
import pandas as pd
import requests
from io import StringIO
from datetime import datetime
import pytz

from dotenv import load_dotenv
from utils.mysql_connect_funcs import get_df_tblName, insert_row_FR
from utils.OpenAI_functions import run_assistant

print("---------- Starting: ASX/ASX_download3Y.py ----------")

load_dotenv()

# Proxy setup
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

def format_dollars(value):
    try:
        value = str(value)
        value = re.sub(r"[^\d.]", "", value)
        return f"${float(value):,.2f}" if value else "NA"
    except:
        return "NA"

def sanitize_filename(name):
    return re.sub(r'[<>:"/\\|?*]', '', name)

def is_file_in_use(file_path):
    """Check if the file is in use by another process"""
    try:
        with open(file_path, 'rb'):
            return False
    except PermissionError:
        return True

def download_docs_from_asx(symbol, name, links, dates, parent_path):
    os.makedirs(os.path.join(parent_path, symbol), exist_ok=True)

    for i, url in enumerate(links):
        response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        if response.status_code == 200:
            clean_name = sanitize_filename(name[i])
            cleaned_date = sanitize_filename(dates[i])
            file_path = os.path.join(parent_path, symbol, f"{symbol} {cleaned_date} {clean_name}.pdf")

            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {file_path}")

            time.sleep(2)  # To avoid rapid requests
        else:
            print(f"Failed to download PDF for {symbol} {name[i]}")

# Fetch announcements
df = get_df_tblName("announcements_difference")

# Filter based on keywords
filter_values = ["appendix 3y", "change of director's interest"]
df = df[df["Document Name"].str.contains("|".join(filter_values), case=False, na=False) |
        df["Type"].str.contains("|".join(filter_values), case=False, na=False)]
df = df.reset_index(drop=True)

if not df.empty:
    for i in range(len(df)):
        url = df.at[i, 'Links']
        try:
            response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
            response.raise_for_status()

            symbol = df.at[i, 'Ticker']
            clean_name = sanitize_filename(df.at[i, "Document Name"])
            cleaned_date = sanitize_filename(df.at[i, "Date"])
            file_path = os.path.join(parent_path, 'Appendix_3Y', f"{i} {symbol} {cleaned_date} {clean_name}.pdf")

            with open(file_path, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {file_path}")

            time.sleep(2)

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")

    # Process downloaded files
    file_paths = sorted([
        os.path.join(root, file)
        for root, _, files in os.walk(os.path.join(parent_path, 'Appendix_3Y'))
        for file in files
    ])

    sydney_time = datetime.now(pytz.timezone("Australia/Sydney"))
    date = sydney_time.strftime("%d %b %Y")

    assistantID = "asst_itFTGt6IWtVjfqCBrdVMfLdk"
    prompt = """
    The file attached is a form submitted to the ASX about a director's change in stock holdings. 
    Extract the following details for each transaction:
    - Director name
    - Whether stock was bought, sold, or issued
    - Number of shares transacted
    - Price per share
    - Total value of the transaction
    - Type of transaction (e.g., on-market, issued, etc.)

    Return the information in TSV format with the columns:
    Director, Bought/Sold/Issued, Number of Shares, Price per Share, Value, Type.

    Only return the TSV content with column headers.
    """

    dfs = []
    for i, file_path in enumerate(file_paths):
        try:
            tsv_string = run_assistant(file_path, prompt, assistantID)
            cleaned_tsv = "\n".join([line for line in tsv_string.split("\n") if not re.fullmatch(r"\W+", line)])
            df_tsv = pd.read_csv(StringIO(cleaned_tsv), sep="\t")

            if not df_tsv.empty and df_tsv.shape[1] == 6:
                df_tsv.columns = ['Director', 'Bought/Sold/Issued', 'Number of Shares', 'Price per Share', 'Value', 'Type']
                df_tsv.insert(0, "Date", df.at[i, 'Date'])
                df_tsv.insert(1, "Ticker", df.at[i, 'Ticker'])
                dfs.append(df_tsv)
        except Exception as e:
            print(f"An error has occurred: {e}")

    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df['Price per Share'] = combined_df['Price per Share'].apply(format_dollars)
        combined_df['Value'] = combined_df['Value'].apply(format_dollars)
        combined_df.columns = ['Date', 'Ticker', 'Director', 'Type', 'Number of Shares', 'Price per Share', 'Value', 'Notes']
        combined_df['Ticker'] = combined_df['Ticker'].apply(lambda x: f"[{x}](/02-companyoverview?data={x}_AU)")

        for i in range(len(combined_df)):
            row = combined_df.iloc[i].tolist()
            insert_row_FR("insiderTrades_today", row, ['Date', 'Ticker', 'Director', 'Type', 'Number of Shares', 'Price per Share', 'Value', 'Notes'])
            insert_row_FR("insiderTrades_total", row, ['Date', 'Ticker', 'Director', 'Type', 'Number of Shares', 'Price per Share', 'Value', 'Notes'])

    # Ensure files are closed before deleting
    time.sleep(3)

    for filename in os.listdir(os.path.join(parent_path, 'Appendix_3Y')):
        file_path = os.path.join(parent_path, 'Appendix_3Y', filename)
        if os.path.isfile(file_path) and filename != "init.txt":
            retry_count = 0
            while is_file_in_use(file_path) and retry_count < 5:
                #print(f"Waiting for {filename} to be released...")
                time.sleep(2)
                retry_count += 1

            try:
                os.remove(file_path)
                print(f"Deleted {filename}")
            except PermissionError:
                print(f"Failed to delete {filename}, skipping.")

else:
    print("No Insider Trades")

print("---------- Finished: ASX/ASX_download3Y.py ----------\n\n\n")