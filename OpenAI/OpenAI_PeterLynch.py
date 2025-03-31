import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.mysql_connect_funcs import get_df_tblName, replace_row
import os
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = api_key
client = OpenAI()

def select_last_3_yrs(df):
    if len(df.columns) >= 4:
        # Select the first column and the last 5 columns
        return df.iloc[:, [0] + list(range(-3, 0))]
    else:
        # Select the entire DataFrame if it has fewer than 6 columns
        return df

def get_analysis(stock):
    #description & financials
    df = get_df_tblName('metadataTBL')
    description = df.loc[df['symbol'] == stock, 'description'].iloc[0]
    df_IS = get_df_tblName(stock + "_AU_annual_income_statement")
    df_BS = get_df_tblName(stock + "_AU_annual_balance_sheet")
    df_IS = df_IS[df_IS['Item'].str.contains('Revenue|Net Income', case=False, na=False, regex=False)]
    df_BS = df_BS[df_BS['Item'].str.contains('Cash & Cash Equivalent|Plant, Property & Equipment (Net)|Total Assets|Long Term Debt|Total Liabilities', case=False, na=False, regex=False)]
    df_c = pd.concat([df_IS, df_BS], ignore_index=True)
    df_c = select_last_3_yrs(df_c)
    tsv_fin = df_c.to_csv(sep="\t", index=False)

    #history
    df_history = get_df_tblName('History_short')
    df_history = df_history[df_history['ticker'] == stock]
    df_history = df_history.sort_values(by='year', ascending=False)
    df_history = df_history.head(3)
    tsv_history = df_history.to_csv(sep="\t", index=False)

    content = """Peter Lynch, in his book One Up on Wall Street, categorizes stocks into six main types based on their characteristics, growth potential, and risk. Here's a summary of each category:
    1. Slow Growers
    Definition: Large, established companies with slow, consistent growth, typically in the range of 2-4% annually.
    Characteristics:
    Often pay high dividends.
    Mature companies in mature industries.
    Limited potential for capital appreciation.
    Often utility companies or large conglomerates.
    Investment Strategy: Suitable for conservative investors seeking steady income and low risk.
    2. Stalwarts
    Definition: Large, well-established companies that grow at a moderate pace, typically 10-12% annually.
    Characteristics:
    Strong balance sheets and consistent earnings.
    Often household names (e.g., Coca-Cola, Procter & Gamble).
    Less volatile than the market but still offer reasonable growth.
    Investment Strategy: Good for long-term, low-risk investments with moderate returns.
    3. Fast Growers
    Definition: Small, aggressive companies growing earnings at 20% or more annually.
    Characteristics:
    Often in emerging industries or expanding into new markets.
    Reinvest most of their profits to fuel growth.
    Higher risk but significant potential for capital appreciation.
    Investment Strategy: Best for risk-tolerant investors seeking high growth and willing to monitor the company closely.
    4. Cyclicals
    Definition: Companies whose fortunes are closely tied to the economic cycle.
    Characteristics:
    Earnings fluctuate significantly with economic conditions.
    Common in industries like automobiles, airlines, and steel.
    Perform well during economic expansions but suffer during recessions.
    Investment Strategy: Timing is crucial. Buy when the cycle is at a low point and sell near the peak.
    5. Turnarounds
    Definition: Companies that are currently struggling but have potential for recovery.
    Characteristics:
    Often facing financial or operational difficulties.
    High risk but can offer significant rewards if recovery is successful.
    Examples include companies emerging from bankruptcy or undergoing restructuring.
    Investment Strategy: Requires deep research to assess the likelihood of a successful turnaround.
    6. Asset Plays
    Definition: Companies that hold valuable assets that the market has overlooked.
    Characteristics:
    Assets could be real estate, natural resources, patents, or a valuable subsidiary.
    Often undervalued by the market relative to their asset value.
    Investment Strategy: Suitable for value investors who can identify hidden assets and wait for the market to recognize their true value.
    \nBased on these definitions and the information below, what would the primary classification for this company be and why? \n""" + "Description of the company:\n" + description + "\n Financials in millions:\n" + tsv_fin + "\n History of the company:\n" + tsv_history

    completion = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
      {"role": "user", "content": content}
    ]
    )

    return completion.choices[0].message.content

def turn_to_tsv(result):
  content = "Please convert the information below into a summary table and return it in TSV format. The summary table should have a metrics column and a characteristics column. The metrics should be: classification, Growth Drivers, Risk Factors, Investor Suitability and Outlook. Be fairly detailed with all the metrics other than Classification. Do not give me anything other than the table itself."   "\n Here is the text I want you to turn into a table:\n" + result
  completion = client.chat.completions.create(
  model="gpt-4o-mini",
  messages=[
    {"role": "user", "content": content}
  ]
  )
  return completion.choices[0].message.content


def find_category(content):
    if 'Slow Grower' in content:
        category = 'Slow Grower'
    elif 'Stalwart' in content:
        category = 'Stalwart'
    elif 'Fast Grower' in content:
        category = 'Fast Grower'
    elif 'Cyclical' in content:
        category = 'Cyclical'
    elif 'Turnaround' in content:
        category = 'Turnarounds'
    elif 'Asset Play' in content:
        category = 'Asset Play'
    else:
        category = ''
    return category


docs_path = "../annual_reports/"
tickers = [folder for folder in os.listdir(docs_path) if os.path.isdir(os.path.join(docs_path, folder))]

if not tickers:
    print('No updates')
else:
    for i in range(len(tickers)):
        try:
            message = get_analysis(tickers[i])
            row = [tickers[i], message]
            replace_row('Peter_Lynch_Analysis', row, ['ticker', 'content'])

            tsv = turn_to_tsv(message)
            row = [tickers[i], tsv]
            replace_row('Peter_Lynch_Summary_TBL', row, ['ticker', 'content'])

            category = find_category(tsv)
            row = [tickers[i], category]
            replace_row('Peter_Lynch_Category', row, ['ticker', 'content'])
            print(str(i) + ' ' + tickers[i] + ' succeeded')
        except:
            print(str(i) + ' ' + tickers[i] + ' failed')

