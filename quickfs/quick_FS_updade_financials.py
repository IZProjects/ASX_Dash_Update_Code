import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.quickFS_functions import get_companies_new_updates, get_metrics, get_financials
import pytz
import pandas as pd
from datetime import datetime
from utils.mysql_connect_funcs import write_df_tblName
pd.options.mode.chained_assignment = None  # default='warn'

sydney_tz = pytz.timezone("Australia/Sydney")
sydney_time = datetime.now(sydney_tz)
sydney_time = sydney_time.strftime("%Y%m%d")
stocks = get_companies_new_updates(sydney_time,"AU")

#misc
misc = ['Share Count', 'Dividends', 'Period End Price', 'Period End Date', 'Original Filing Date', 'Restated Filing Date', 'Preliminary']

#Computed
computed = ['EBITDA', 'Free Cash Flow', 'Book Value', 'Tangible Book Value', 'Return on Assets', 'Return on Equity', 'Return on Invested Capital', 'Return on Capital Employed',
 'Return on Average Tangible Common Equity', 'Gross Margin', 'EBITDA Margin', 'Operating Margin', 'Pretax Margin', 'Net Income Margin', 'Free Cash Flow Margin', 'Asset to Equity',
 'Equity to Assets', 'Debt to Equity', 'Debt to Assets', 'Revenue per Share', 'EBITDA per Share', 'Operating Income per Share', 'Pretax Income per Share', 'Free Cash Flow per Share',
 'Book Value per Share', 'Tangible Book Value per Share', 'Market Cap', 'Enterprise Value', 'Price to Earnings', 'Price to Book Value', 'Price to Tangible Book Value', 'Price to Sales',
 'Price to Free Cash Flow', 'Price to Pretax Income', 'Enterprise Value to Earnings', 'Enterprise Value to Book Value', 'Enterprise Value to Tangible Book Value',
 'Enterprise Value to Sales', 'Enterprise Value to Free Cash Flow', 'Enterprise Value to Pretax Income', 'Revenue Growth', 'Gross Profit Growth', 'EBITDA Growth', 'Operating Income Growth',
 'Pre-Tax Income Growth', 'Net Income Growth', 'Diluted EPS Growth', 'Number of Diluted Shares Growth', 'Cash & Cash Equivilants Growth', 'Plant, Property and Equipment Growth', 'Total Asset Growth',
 'Total Equity Growth', 'Operating Cash Flow Growth', 'Capital Expenditure Growth', 'Free Cash Flow Growth', '10 Yr Revenue CAGR', '10 Yr Diluted EPS CAGR', '10 Yr Total Assets CAGR',
 '10 Yr Total Equity CAGR', '10 Year Free Cash Flow CAGR', 'Payout Ratio', 'Median Gross Margin', 'Median Pre-tax Margin', 'Median Operating Income Margin', 'Median Free Cash Flow Margin',
 'Median Return on Assets', 'Median Return on Equity', 'Median Return on Invested Capital', 'Median Asset to Equity', 'Median Debt to Asset', 'Median Debt to Equity', 'Earning Assets',
 'Net Interest Margin', 'Earning Asset to Equity', 'Loans to Deposits', 'Loan Loss Reserve to Loans', 'Net Interest Income Growth', 'Gross Loans Growth', 'Net Loans Growth', 'Deposits Growth',
 'Earning Assets Growth', '10 Yr Net Interest Income CAGR', '10 Yr Gross Loan CARG', '10 Yr Earning Assets CAGR', '10 Yr Deposits CAGR', 'Median Net Interest Margin',
 'Median Earnings Assets to Equity', 'Median Equity to Assets', 'Median Loans to Deposits', 'Median Loan Loss Reserve to Loans', 'Policy Revenue', 'Underwriting Profit', 'Return on Investment',
 'Underwriting Margin', 'Premiums per Share', 'Premiums Growth', 'Policy Revenue Growth', 'Total Investments Growth', '10 Yr Premiums CAGR', '10 Yr Total Investments CAGR',
 'Median Return on Investment', 'Median Underwriting Margin', 'Income Tax Rate', 'Net Operating Profit After Tax', 'Return on Invested Capital 2', 'Dividends per Share Growth', 'Shares EOP Growth',
 '10 Yr Dividends per Share CAGR', '10 Yr Operating Cash Flow CAGR','Current Ratio', 'Price to Earnings Growth Ratio', 'Shares EOP Change', 'Net Debt', 'Average 5 Yr Return on Invested Capital']

sup_IS = ['EBITDA', 'Gross Margin', 'EBITDA Margin', 'Operating Margin', 'Pretax Margin', 'Net Income Margin', 'Revenue per Share', 'EBITDA per Share', 'Operating Income per Share', 'Pretax Income per Share',
          'Revenue Growth', 'Gross Profit Growth', 'EBITDA Growth', 'Operating Income Growth', 'Pre-Tax Income Growth', 'Net Income Growth', 'Diluted EPS Growth', 'Number of Diluted Shares Growth', 'Net Interest Margin',
          'Net Interest Income Growth','10 Yr Revenue CAGR', '10 Yr Diluted EPS CAGR', '10 Yr Net Interest Income CAGR','Policy Revenue', 'Underwriting Profit','Underwriting Margin', 'Policy Revenue Growth',
          'Income Tax Rate', 'Net Operating Profit After Tax', 'Share Count', 'Market Cap', 'Enterprise Value']

sup_BS = ['Book Value', 'Tangible Book Value','Asset to Equity','Equity to Assets', 'Debt to Equity', 'Debt to Assets', 'Cash & Cash Equivilants Growth', 'Plant, Property and Equipment Growth', 'Total Asset Growth',
          'Total Equity Growth', 'Capital Expenditure Growth', '10 Yr Total Assets CAGR','10 Yr Total Equity CAGR', 'Earning Assets', 'Earning Asset to Equity', 'Loans to Deposits', 'Loan Loss Reserve to Loans',
          'Gross Loans Growth', 'Net Loans Growth', 'Deposits Growth','Earning Assets Growth', '10 Yr Earning Assets CAGR', '10 Yr Deposits CAGR','Premiums per Share', 'Premiums Growth',
          'Total Investments Growth', '10 Yr Premiums CAGR', '10 Yr Total Investments CAGR', 'Current Ratio', 'Net Debt']

sup_CF = ['Free Cash Flow Margin','Free Cash Flow per Share', 'Free Cash Flow Growth','Operating Cash Flow Growth','Capital Expenditure Growth','Payout Ratio','Dividends per Share Growth', '10 Yr Dividends per Share CAGR',
          '10 Year Free Cash Flow CAGR','10 Yr Operating Cash Flow CAGR']

key_ratios = ['Return on Assets', 'Return on Equity', 'Return on Invested Capital', 'Return on Capital Employed','Return on Average Tangible Common Equity','Price to Earnings', 'Price to Book Value', 'Price to Tangible Book Value', 'Price to Sales',
              'Price to Free Cash Flow', 'Price to Pretax Income', 'Enterprise Value to Earnings', 'Enterprise Value to Book Value', 'Enterprise Value to Tangible Book Value','Enterprise Value to Sales',
              'Enterprise Value to Free Cash Flow', 'Enterprise Value to Pretax Income','Number of Diluted Shares Growth','Return on Investment', 'Average 5 Yr Return on Invested Capital']

summary = ['Market Cap', 'Enterprise Value','Share Count','Price to Earnings', 'Price to Book Value','Enterprise Value to Free Cash Flow','Debt to Equity','Net Debt','Current Ratio','Revenue Growth','EBITDA Growth','Net Income Growth',
           'Return on Equity','Return on Assets','Return on Invested Capital','Gross Margin','Operating Margin','Net Income Margin']

summary_rename = ['Market Cap', 'EV','Share Count','P/E', 'P/BV','EV/FCF','D/E','Net Debt','Current Ratio','Revenue Growth (Y/Y)','EBITDA Growth (Y/Y)','Net Income Growth (Y/Y)',
           'ROE','ROA','ROIC','Gross Margin','Operating Margin','Net Income Margin']

#cash flow statement
CF = ['Net Income', 'Depreciation & Amortization', 'Accounts Receivable', 'Inventory', 'Prepaid Expenses', 'Other Working Capital', 'Change in Working Capital', 'Deferred Tax',
 'Stock Compensation', 'Other Non-Cash Items', 'Cash from Operations', 'Plant, Property & Equipment (Net)', 'Acquisitions (Net)', 'Investments (Net)', 'Intangibles (Net)', 'Other', 'Cash from Investing',
 'Net Common Stock Issued', 'Net Preferred Stock Issued', 'Net Debt Issued', 'Dividends Paid', 'Cash from Financing', 'Forex', 'Net Change in Cash',
 'Capital Expenditure', 'Purchases of Plant, Property & Equipment', 'Sales of Plant, Property & Equipment', 'Acquisitions', 'Divestitures', 'Investment Purchases', 'Investment Sales',
 'Common Stock Issued', 'Common Stock Repurchased', 'Preferred Stock Issued', 'Preferred Stock Repurchased', 'Debt Issued', 'Debt Repaid']

#balance sheet
BS = ['Cash & Cash Equivalent', 'Short Term Investments', 'Receivables', 'Inventories', 'Other Current Assets', 'Total Current Assets', 'Equity & Other Investments', 'Plant, Property & Equipment (Gross)',
      'Accumulated Depreciation', 'Plant, Property & Equipment (Net)', 'Intangible Assets', 'Goodwill', 'Other Long Term Assets', 'Total Assets', 'Accounts Payble', 'Tax Payable', 'Current Accrued Liabilities', 'Short Term Debt',
      'Current Deferred Revenue', 'Current Deferred Tax Liability', 'Long Term Debt', 'Capital Leases', 'Pension Liabilities', 'Non-current Deferred Revenue', 'Other Non-Current Liabilities',
      'Total Liabilities', 'Common Stock', 'Preferred Stock', 'Retained Earnings', 'Accumulated Other Comprehensive Income', 'Additional Paid-In Capital', 'Treasury Stock', 'Other Equity', 'Minority Interest Liability',
      'Total Equity', 'Total Liabilities & Equity', 'Long Term Debt & Capital Lease Obligations', 'Loans (gross)', 'Allowance For Loan Losses', 'Loans (Net)','Total Investments', 'Deposit Liability', 'Deferred Policy Acquisition Cost', 'Unearned Premiums',
      'Future Policy Benifits', 'Current Capital Leases', 'Total Non-Current Assets', 'Total Non-Current Liabilities', 'Unearned Income']

#income statement
IS = ['Revenue', 'Cost of Goods Sold', 'Gross Profit', 'Selling, General and Admin Expenses', 'Research & Development', 'One-time Charges', 'Other Operating Expenses', 'Total Operating Expenses',
 'Operating Income', 'Net Interest Income', 'Other Non-Operating Expense', 'Pre-tax Income', 'Income Tax', 'Net Income from Continuing Operations', 'Interest Income', 'Interest Expense',
 'Net Income from Discontinued Operations', 'Income Allocated to Minority Interest', 'Other', 'Net Income', 'Preferred Dividends', 'Net Income Avaliable to Shareholders', 'EPS - Basic',
 'EPS - Diluted', 'Number of Basic Shares', 'Number of Shares Diluted', 'Total Interest Income', 'Total Interest Expense', 'Total Non-Interest Revenue',
 'Provision for Credit Losses', 'Net Income after Credit Loss Provisions', 'Total Non-Interest Expense', 'Premiums Earned', 'Net Investment Income', 'Fees and Other Income',
 'Net Policy Holder Claims Expense', 'Policy Acquasition Expense']

boldIS = ['Revenue','Gross Profit','Operating Income','Net Income from Continuing Operations','Net Income from Discontinued Operations','Net Income','EPS - Basic','EPS - Diluted']
boldBS = ['Total Current Assets','Total Assets','Total Liabilities','Total Equity', 'Total Liabilities & Equity','Total Non-Current Assets', 'Total Non-Current Liabilities']
boldCF = ['Cash from Operations','Cash from Investing','Cash from Financing','Net Change in Cash']
underlineIS = ['Revenue','Gross Profit','Operating Income','Net Income','EPS - Diluted']
underlineBS = ['Total Assets','Total Liabilities','Total Equity', 'Total Liabilities & Equity']
underlineCF = ['Cash from Operations','Cash from Investing','Cash from Financing','Net Change in Cash']

#for operations
def get_statement(df_metrics, df, statement_type, rename_list):
  # statement_type = 'income_statement' / 'balance_sheet' / 'cash_flow_statement' / 'computed' / 'misc'
  filtered_metric = df_metrics[df_metrics['statement_type'] == statement_type]
  metrics_o = filtered_metric['metric'].to_list()
  metrics = [col for col in metrics_o if col in df.columns]
  metrics = ['period_end_date'] + metrics
  df_result = df[metrics]
  df_transposed = df_result.transpose()
  df_transposed = df_transposed.reset_index()
  df_transposed.columns = df_transposed.iloc[0]
  df_transposed = df_transposed[1:].reset_index(drop=True)
  df_transposed = df_transposed.rename(columns={df_transposed.columns[0]: 'Item'})
  mapping_dict = dict(zip(metrics_o, rename_list))
  df_transposed['Item'] = df_transposed['Item'].map(mapping_dict)
  condition = (df_transposed.iloc[:, 1:] == 0).all(axis=1)
  df_transposed = df_transposed[~condition]
  return df_transposed

def get_supplementary_data(df_metrics, df, rename_list):
  df1 = get_statement(df_metrics, df, "computed", computed)
  df2 = get_statement(df_metrics, df, "misc", misc)
  df = pd.concat([df1, df2], ignore_index=True)
  df_filtered = df[df['Item'].isin(rename_list)]
  df_filtered['Item'] = pd.Categorical(df_filtered['Item'], categories=rename_list, ordered=True)
  df_filtered = df_filtered.sort_values('Item').reset_index(drop=True)
  return df_filtered

def convert_to_percentage(value):
    return f"{float(value) * 100:.2f}%"

statement_types = [('income_statement',IS), ('balance_sheet',BS), ('cash_flow_statement',CF)]
supplementary_types = [('sup_IS',sup_IS), ('sup_BS',sup_BS), ('sup_CF',sup_CF), ('key_ratios',key_ratios), ('summary',summary)]
period_types = ['annual', 'quarterly']

# get individial statements
fails = []
df_metrics = get_metrics()
for stock in stocks:
  for period in period_types:
    df_financials = get_financials(stock, period)
    if df_financials is not None:
      for statement in statement_types:
        try:
          df1 = get_statement(df_metrics, df_financials, statement[0], statement[1])
          df1 = df1.replace('-', '0')
          df1 = df1.dropna()
          df1 = df1.reset_index(drop=True)
          stockName = stock.replace(":", "_")
          name = stockName + '_' + period + '_' + statement[0]
          #df1.to_sql(name, db_conn, if_exists='replace', index=False)
          write_df_tblName(name,df1)
        except:
          fails.append([stock,period,statement[0]])

      for sup in supplementary_types:
        try:
          df2 = get_supplementary_data(df_metrics, df_financials, sup[1])
          if sup[0] == 'summary':
            df2 = df2.iloc[:, [0, -1]]
          cols=df2.columns.to_list()
          missing_items = [item for item in sup[1] if item not in df2['Item'].values]
          missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
          df2 = pd.concat([df2, missing_df], ignore_index=True)
          df2['Item'] = pd.Categorical(df2['Item'], categories=sup[1], ordered=True)
          df2 = df2.sort_values('Item').reset_index(drop=True)

          if sup[0] == 'summary':
            df2['Item'] = summary_rename

          df2 = df2.replace('-', '0')
          df2 = df2.dropna()
          df2 = df2.reset_index(drop=True)
          mask = df2['Item'].str.contains('Margin|Growth|CAGR|Return', regex=True)
          columns_to_convert = df2.columns[1:]
          df2.loc[mask, columns_to_convert] = df2.loc[mask, columns_to_convert].apply(lambda x: x.map(convert_to_percentage).astype(str))

          stockName = stock.replace(":", "_")
          name = stockName + '_' + period + '_' + sup[0]
          #df2.to_sql(name, db_conn, if_exists='replace', index=False)
          write_df_tblName(name,df2)
        except:
          fails.append([stock,period,sup[0]])

df_fails = pd.DataFrame(fails, columns=['stock', 'period', 'statement_type'])
print(df_fails)