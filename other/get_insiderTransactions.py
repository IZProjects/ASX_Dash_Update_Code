from selenium import webdriver
import pandas as pd
from bs4 import BeautifulSoup
from utils.mysql_connect_funcs import write_df_tblName, filter_table

user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'


url = 'https://www.marketindex.com.au/director-transactions'

options = webdriver.ChromeOptions()
options.add_argument(f'user-agent={user_agent}')
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver=webdriver.Chrome(options=options)
driver.set_window_size(1080, 800)
driver.get(url)
driver.implicitly_wait(30)

soup = BeautifulSoup(driver.page_source, "lxml")
tables = soup.find("div", {"id": "sticky-table"})
rows = tables.find_all("tr")

html = []
for row in rows:
  ticker = row.find_all("td", {"class": "sticky-column"})
  html.append(row.find_all("td"))

list_of_list = []
for line in html:
  sublist = []
  for words in line:
    sublist.append(words.text)
  list_of_list.append(sublist)

rows = [sublist for sublist in list_of_list if sublist]
df = pd.DataFrame(rows, columns=['Code', 'Company', 'Date', 'Director', 'Type', 'Amount', 'Price', 'Value', 'Notes'])

tickers = df['Code'].to_list()
tickers = [ticker + ".AU" for ticker in tickers]
df_prices = filter_table('real_time', 'code', tickers)
df_prices['code'] = df_prices['code'].str.replace('.AU', '', regex=False)
df_prices['change_p'] = df_prices['change_p'].apply(lambda x: x[:-2] if len(x) > 5 else x)
df['Price Change (%)'] = df['Code'].map(df_prices.set_index('code')['change_p'])
df = df.dropna().drop_duplicates().reset_index(drop=True)
df['Price Change (%)'] = df['Price Change (%)'].astype(float)


write_df_tblName("insiderTrades_today", df)

print(df)