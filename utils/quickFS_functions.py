import requests
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
pd.options.mode.chained_assignment = None  # default='warn'

load_dotenv()
API_KEY = os.getenv("quickFS_API_KEY")

def get_companies(country,exchange):
    base_url = 'https://public-api.quickfs.net/v1/companies'

    if country != '' and exchange != '':
        url = f"{base_url}/{country}/{exchange}"
    elif country != '' and exchange == '':
        url = f"{base_url}/{country}"
    else:
        url = f"{base_url}/{exchange}"

    headers = {
        'X-QFS-API-Key': API_KEY
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        j = response.json()
        data = j['data']
        return data
    else:
        print(f"Error: {response.status_code}")
        return None


def get_companies_new_updates(date=None,country=None):
    base_url = 'https://public-api.quickfs.net/v1/companies/updated'

    url = f"{base_url}/{date}/{country}"

    headers = {
        'X-QFS-API-Key': API_KEY
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        j = response.json()
        data = j['data']
        return data
    else:
        print(f"Error: {response.status_code}")
        return None

def get_metrics():
    url = 'https://public-api.quickfs.net/v1/metrics'

    headers = {
        'X-QFS-API-Key': API_KEY
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        j = response.json()
        data = j['data']
        df = pd.DataFrame(data)
        return df
    else:
        print(f"Error: {response.status_code}")
        return None

def get_financials(symbol,period):
  #type = annual or quarterly
    base_url = 'https://public-api.quickfs.net/v1/data/all-data'

    url = f"{base_url}/{symbol}"

    headers = {
        'X-QFS-API-Key': API_KEY
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        j = response.json()
        data = j['data']['financials'][period]

        max_length = max(len(v) for v in data.values() if isinstance(v, list))

        if max_length !=0:
          for key, value in data.items():
              # Check if the value is not a list
              if not isinstance(value, list):
                  # Convert the value to a list and pad with '0'
                  data[key] = [value] + [np.nan] * (max_length - 1)
              elif len(value) < max_length:
                  pad_length = max_length - len(value)
                  value.extend([np.nan] * pad_length)
              else:
                  # Pad the list with '0' to make its length equal to max_length
                  data[key] = value + [np.nan] * (max_length - len(value))

          df = pd.DataFrame(data)
          return df
        else:
          return None
    else:
        print(f"Error: {response.status_code}")
        return None

def get_metadata(symbol):
  #type = annual or quarterly
    base_url = 'https://public-api.quickfs.net/v1/data/all-data'

    url = f"{base_url}/{symbol}"

    headers = {
        'X-QFS-API-Key': API_KEY
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        j = response.json()
        data = j['data']['metadata']
        df = pd.DataFrame([data])
        return df
    else:
        print(f"Error: {response.status_code}")
        return None

