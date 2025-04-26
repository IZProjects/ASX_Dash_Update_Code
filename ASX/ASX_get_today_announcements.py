import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.ASX_functions import getAnnouncements
from utils.mysql_connect_funcs import get_df_tblName, write_df_tblName, filter_table
from utils.AWS_functions import upload_file
from datetime import datetime
import pytz
import pandas as pd
import numpy as np
import time
from dotenv import load_dotenv

load_dotenv()

s3_bucket = os.getenv("AWS_S3_BUCKET_NAME")
s3_folder = os.getenv("AWS_S3_FOLDER")

print("---------- Starting: ASX/ASX_get_today_announcements.py ----------")

sydney_tz = pytz.timezone("Australia/Sydney")
sydney_time = datetime.now(sydney_tz)
date = sydney_time.strftime("%d %b %Y")

url = 'https://www2.asx.com.au/markets/trade-our-cash-market/announcements'
#url = 'https://www2.asx.com.au/markets/trade-our-cash-market/announcements.jdo.au'

#df_existing = getAnnouncements(url, 1)
df_existing = get_df_tblName('announcements_today')
df_existing2 = get_df_tblName('announcements_today_wPrice')
df_metadata = get_df_tblName("metadataTBL")

driver = None
max_retries = 10
for attempt in range(1, max_retries + 1):
    try:
        df_new, driver = getAnnouncements(url, 3)
        df_new.columns = [col.title() for col in df_new.columns]

        df_new = df_new[df_new["Date"].str.contains(date, na=False)]
        df_new = df_new[df_new["Ticker"].isin(df_metadata["symbol"])]

        df_difference = df_new.merge(df_existing, how="left", indicator=True).query('_merge == "left_only"').drop(columns=["_merge"])
        df_difference = df_difference.reset_index(drop=True)
        df_differenceTBL = df_difference.copy()


        df_combined = pd.concat([df_existing, df_difference]).reset_index(drop=True)
        df_combinedTBL = df_combined.copy()


        tickers = df_difference['Ticker'].to_list()
        tickers = [ticker + ".AU" for ticker in tickers]
        df_prices = filter_table('real_time', 'code', tickers)
        df_prices['code'] = df_prices['code'].str.replace('.AU', '', regex=False)
        df_prices['change_p'] = df_prices['change_p'].apply(lambda x: x[:-2] if len(x) > 5 else x)
        df_difference['Price Change (%)'] = df_difference['Ticker'].map(df_prices.set_index('code')['change_p'])
        df_difference = df_difference.dropna().drop_duplicates().reset_index(drop=True)
        df_difference['Document Name'] = df_difference['Document Name'].str.upper()
        df_difference['Type'] = df_difference['Type'].str.upper()
        df_difference['Price Change (%)'] = df_difference['Price Change (%)'].astype(float)
        df_difference['Ticker'] = df_difference['Ticker'].apply(lambda x: f'[{x}](/02-companyoverview?data={x}_AU)')

        existing_numbers = set(df_existing2['Document Number'].astype(int))
        all_possible = set(range(1, 10000))
        available_numbers = list(all_possible - existing_numbers)
        if len(df_difference) > len(available_numbers):
            raise ValueError("Not enough unique document numbers available.")
        new_numbers = np.random.choice(available_numbers, size=len(df_difference), replace=False)
        df_difference['Document Number'] = [str(num).zfill(4) for num in new_numbers]

        awsLinks = []
        for i in range(len(df_difference)):
            url = df_difference.at[i,'Links']
            filename = str(df_difference.at[i,'Document Number'])
            upload_file(url, f'{filename}.pdf')
            awsLinks.append(f"https://{s3_bucket}.s3.ap-southeast-2.amazonaws.com/{s3_folder}/{filename}.pdf")

        df_difference['awsLinks'] = awsLinks

        df_difference["Document Name"] = df_difference.apply(lambda row: f'[{row["Document Name"]}]({row["awsLinks"]})', axis=1)
        df_difference = df_difference.drop(columns=["Links"])
        df_difference = df_difference.drop(columns=["awsLinks"])
        cols = ["Date", "Ticker"] + [col for col in df_difference.columns if col not in ["Date", "Ticker"]]
        df_difference = df_difference[cols]

        df_combined2 = pd.concat([df_existing2, df_difference]).reset_index(drop=True)

        write_df_tblName('announcements_difference', df_differenceTBL)
        write_df_tblName('announcements_today', df_combinedTBL)
        write_df_tblName("announcements_today_wPrice", df_combined2)
        print("---------- Finished: ASX/ASX_get_today_announcements.py ----------\n\n\n")
        break


    except Exception as e:
        print(f"Attempt {attempt} failed with error: {e}")
        if attempt == max_retries:
            print("Max retries reached. Exiting.")
        else:
            time.sleep(2)

    finally:
        if driver:
            driver.quit()
            time.sleep(2)

