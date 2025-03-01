import yfinance as yf
import pandas as pd
from utils.mysql_connect_funcs import write_df_tblName
import logging

# Configure logging
logging.basicConfig(
    filename="EODHD_WLA.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

try:
    # List of Australian sector tickers and their corresponding sector names
    australian_sector_tickers = {
        '^AXDJ': 'Consumer Discretionary',
        '^AXSJ': 'Consumer Staples',
        '^AXEJ': 'Energy',
        '^AXFJ': 'Financials',
        '^AXHJ': 'Health Care',
        '^AXIJ': 'Industrials',
        '^AXMJ': 'Materials',
        '^AXRE': 'Real Estate',
        '^AXTJ': 'Information Technology',
        '^AXUJ': 'Utilities'
    }

    # Download data for the most recent trading day
    data = yf.download(list(australian_sector_tickers.keys()), period='1d', interval='15m')

    # Initialize an empty list to store the results
    results = []

    # Extracting the opening and most recent prices
    for ticker, sector in australian_sector_tickers.items():
        opening_price = data['Open'][ticker].iloc[0]  # Use iloc for position-based indexing
        current_price = data['Close'][ticker].iloc[-1]  # Use iloc for position-based indexing
        percent_change = ((current_price - opening_price) / opening_price) * 100
        results.append({
            'Sector': sector,
            'Price': round(current_price, 2),
            'Change': str(round(percent_change, 2))+'%'
        })

    # Create a DataFrame from the results
    df_results = pd.DataFrame(results)
    df_results['Price'] = df_results['Price'].apply(lambda x: f"{x:,}")

    write_df_tblName("AU_Sectors", df_results)

    logging.info("Data successfully written to the database.")
    print("Data successfully written to the database.")

except Exception as e:
    logging.error(f"An error has occurred: {e}", exc_info=True)
    print(f"An error has occurred: {e}")