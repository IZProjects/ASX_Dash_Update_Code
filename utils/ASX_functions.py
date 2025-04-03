from bs4 import BeautifulSoup
#from selenium import webdriver
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
import pandas as pd
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

load_dotenv()

proxy_host = os.getenv("proxy_host")
proxy_port = os.getenv("proxy_port")
proxy_user = os.getenv("proxy_user")
proxy_pass = os.getenv("proxy_pass")

"""def connect_to_page(url):
  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument('--ignore-certificate-errors')  # <-- Ignore SSL errors
  options.add_argument('--allow-insecure-localhost')  # <-- Allow insecure connections
  driver=webdriver.Chrome(options=options)
  driver.get(url)
  driver.implicitly_wait(10)
  cookies=driver.find_element(by=By.ID,value="onetrust-accept-btn-handler")
  cookies.click()
  #wait_row = WebDriverWait(driver, 3)
  #rows = wait_row.until(EC.presence_of_all_elements_located((By.XPATH, './/*[@class="table table-bordered"]/tbody')))
  #driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
  time.sleep(3)
  return driver"""


# IPRoyal Proxy Credentials
proxy = f"{proxy_host}:{proxy_port}"
proxy_auth = f"{proxy_user}:{proxy_pass}"

def connect_to_page(url):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--ignore-certificate-errors')  # <-- Ignore SSL errors
    options.add_argument('--allow-insecure-localhost')  # <-- Allow insecure connections

    # Configure selenium-wire proxy settings
    seleniumwire_options = {
        'proxy': {
            'http': f'http://{proxy_auth}@{proxy}',
            'https': f'https://{proxy_auth}@{proxy}',
        }
    }

    # Initialize WebDriver with proxy settings
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options, seleniumwire_options=seleniumwire_options)
    driver.get(url)

    # Wait for page to load
    driver.implicitly_wait(10)
    try:
        # Accept cookies if element exists
        cookies = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        cookies.click()
    except Exception as e:
        print("Cookies banner not found or not clickable:", e)

    time.sleep(3)
    return driver


def get_date_replacement(reference):
    today = datetime.now()
    if reference == "Today":
        return today.strftime("%d %b %Y")
    elif reference == "Yesterday":
        yesterday = today - timedelta(days=1)
        return yesterday.strftime("%d %b %Y")
    return reference

def process_date_time(input_string):
    # Replace "Today" and "Yesterday" with actual dates
    input_string = re.sub(r'(Today|Yesterday)', lambda x: get_date_replacement(x.group()), input_string)
    # Separate the year and time
    return re.sub(r'(\d{4})(\d{1,2}:\d{2}[ap]m)', r'\1 \2', input_string)

def getAnnouncementsTBL(driver):
    date = []
    links = []
    documentName = []
    ticker = []
    types = []

    soup = BeautifulSoup(driver.page_source, "lxml")
    table = soup.find("table", {"class": "table table-bordered"})
    for td in table.find_all("td", {"class":"text-muted colDateTime"}):
        text = re.sub(r'\n|\t','', td.text)
        #index = text.rfind(":")
        #text = text[0:index-1] + " " + text[index-1:]
        text = process_date_time(text)
        date.append(text)

    for a in table.find_all("a"):
        #print(a)
        if(a.get("href") != 'javascript:void(0);'):
            links.append(a.get("href"))
            text = re.sub(r'\n|\t', '', a.text)
            documentName.append(text[0:-18].lower())

    for td in table.find_all("ul", {"class":"list list-inline list-comma-delimited m-b-0 hidden-xs"}):
        li = td.find_all('li')
        text=str(li[0])
        text=text[4:-5]
        ticker.append(text)

    for td in table.find_all("ul", {"class":"list"}):
        li = td.find_all('li')
        #print(li[0])
        text=str(li[0])
        text=text[4:-5]
        if(text not in ticker):
            text = re.sub(r'\n|\t', '', text)
            splitIndex = text.find("<")
            types.append(text[0:splitIndex].lower())

    df = pd.DataFrame(zip(date,links,documentName,ticker,types),columns=['date','links','document name','ticker','type'])
    return df

def getAnnouncements(url, noPages):
  driver = connect_to_page(url)

  getTableList = []
  getTableList.append(getAnnouncementsTBL(driver))
  for i in range(noPages):
    try:
      driver.find_element("xpath",'//*[@class="text-upper m-l-3 p-r-2"]').click()
      time.sleep(3)
      getTableList.append(getAnnouncementsTBL(driver))
    except:
      break
  df = pd.concat(getTableList,ignore_index=True)
  #db_path = Paths.database("annoucement_DB.db")
  #connection = sqlite3.connect(db_path)
  #df.columns = ['Date', 'Links', 'Document Name', 'Ticker', 'Type']
  #df.to_sql(sql_table, connection, if_exists='replace', index=False)
  #connection.close()
  #print(sql_table+ " saved")
  return df

