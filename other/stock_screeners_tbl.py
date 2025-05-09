import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import re
from utils.mysql_connect_funcs import fetch_tables_for_screener, get_df_tblName, write_df_tblName
import numpy as np

print("---------- Starting: other/stock_screeners_tbl.py ----------")

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

percentages = ['Gross Margin', 'EBITDA Margin', 'Operating Margin', 'Pretax Margin', 'Net Income Margin', 'Revenue Growth', 'Gross Profit Growth', 'EBITDA Growth', 'Operating Income Growth', 'Pre-Tax Income Growth',
               'Net Income Growth', 'Diluted EPS Growth', 'Number of Diluted Shares Growth', 'Net Interest Margin', 'Net Interest Income Growth',
               'Underwriting Margin', 'Policy Revenue Growth','Income Tax Rate', 'Cash & Cash Equivilants Growth', 'Plant, Property and Equipment Growth', 'Total Asset Growth', 'Total Equity Growth', 'Capital Expenditure Growth',
               'Gross Loans Growth', 'Net Loans Growth', 'Deposits Growth','Earning Assets Growth', 'Premiums Growth',
               'Total Investments Growth', 'Free Cash Flow Margin', 'Free Cash Flow Growth','Operating Cash Flow Growth','Capital Expenditure Growth','Payout Ratio',
               'Dividends per Share Growth', 'Return on Assets', 'Return on Equity', 'Return on Invested Capital', 'Return on Capital Employed',
               'Return on Average Tangible Common Equity', 'Number of Diluted Shares Growth', 'Return on Investment', 'Average 5 Yr Return on Invested Capital']

percentages = [item.replace(' ', '_') for item in percentages]

def sanitize_column_name(col_name):
    return re.sub(r'\W+', '_', col_name)

IS_cols = [IS]
BS_cols = [BS]
CF_cols = [CF]
sup_IS_cols = [sup_IS]
sup_BS_cols = [sup_BS]
sup_CF_cols = [sup_CF]
key_ratios_cols = [key_ratios]

IS_stocks = ['Item']
BS_stocks = ['Item']
CF_stocks = ['Item']
sup_IS_stocks = ['Item']
sup_BS_stocks = ['Item']
sup_CF_stocks = ['Item']
key_ratios_stocks = ['Item']

tables = fetch_tables_for_screener()
for table in tables:
    try:
        df = get_df_tblName(table)
        if 'Item' in df.columns:
            if 'annual_income_statement' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in IS if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=IS, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                IS_cols.append(df.iloc[:, -1].tolist())
                IS_stocks.append(table[0:6])

            elif 'annual_balance_sheet' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in BS if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=BS, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                BS_cols.append(df.iloc[:, -1].tolist())
                BS_stocks.append(table[0:6])

            elif 'annual_cash_flow_statement' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in CF if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=CF, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                CF_cols.append(df.iloc[:, -1].tolist())
                CF_stocks.append(table[0:6])

            elif 'annual_sup_IS' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in sup_IS if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=sup_IS, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                sup_IS_cols.append(df.iloc[:, -1].tolist())
                sup_IS_stocks.append(table[0:6])

            elif 'annual_sup_BS' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in sup_BS if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=sup_BS, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                sup_BS_cols.append(df.iloc[:, -1].tolist())
                sup_BS_stocks.append(table[0:6])

            elif 'annual_sup_CF' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in sup_CF if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=sup_CF, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                sup_CF_cols.append(df.iloc[:, -1].tolist())
                sup_CF_stocks.append(table[0:6])

            elif 'annual_key_ratios' in table:
                cols=df.columns.to_list()
                missing_items = [item for item in key_ratios if item not in df['Item'].values]
                missing_df = pd.DataFrame({'Item': missing_items, cols[-1]: '0'})
                df = pd.concat([df, missing_df], ignore_index=True)
                df['Item'] = pd.Categorical(df['Item'], categories=key_ratios, ordered=True)
                df = df.sort_values('Item').reset_index(drop=True)
                key_ratios_cols.append(df.iloc[:, -1].tolist())
                key_ratios_stocks.append(table[0:6])

            else:
                print("incorrect table input")

    except Exception as e:
        print(f"An error has occurred: {e}")


def check_equal_length(lists):
    return all(len(lst) == len(lists[0]) for lst in lists)

l = [IS_stocks, BS_stocks, CF_stocks, sup_IS_stocks, sup_BS_stocks, sup_CF_stocks, key_ratios_stocks]
if check_equal_length(l):
    print("All lists have the same length.")
else:
    print("Lists have different lengths.")

df_IS = pd.DataFrame(IS_cols).T
df_IS.columns = IS_stocks

df_BS = pd.DataFrame(BS_cols).T
df_BS.columns = BS_stocks

df_CF = pd.DataFrame(CF_cols).T
df_CF.columns = CF_stocks

df_sup_IS = pd.DataFrame(sup_IS_cols).T
df_sup_IS.columns = sup_IS_stocks

df_sup_BS = pd.DataFrame(sup_BS_cols).T
df_sup_BS.columns = sup_BS_stocks

df_sup_CF = pd.DataFrame(sup_CF_cols).T
df_sup_CF.columns = sup_CF_stocks

df_key_ratios = pd.DataFrame(key_ratios_cols).T
df_key_ratios.columns = key_ratios_stocks

df = pd.concat([df_IS, df_BS, df_CF, df_sup_IS, df_sup_BS, df_sup_CF, df_key_ratios], ignore_index=True)

df = df.drop_duplicates()

df_transposed = df.transpose()
df_transposed = df_transposed.reset_index()
df_transposed.columns = df_transposed.iloc[0]
df_transposed = df_transposed[1:].reset_index(drop=True)
df_transposed = df_transposed.rename(columns={df_transposed.columns[0]: 'Item'})
df_transposed = df_transposed.loc[:, ~df_transposed.columns.duplicated()]
df_transposed.columns = df_transposed.columns.str.replace(' ', '_')
df_transposed.columns = [sanitize_column_name(col) for col in df_transposed.columns]

df_metadata = get_df_tblName('metadataTBL')
df_metadata = df_metadata[['symbol', 'name', 'exchange', 'morningstar_industry', 'morningstar_sector']]
df_metadata.columns = ['Item', 'Name', 'Exchange', 'Industry', 'Sector']
df_metadata['Item'] = df_metadata['Item'] + '_AU'

df_transposed = pd.merge(df_transposed, df_metadata, on='Item')
df_transposed = df_transposed.drop_duplicates()
df_transposed = df_transposed.reset_index(drop=True)

#######################
columns = df_transposed.columns.tolist()  # Get all column names as a list
first_column = [columns[0]]    # Keep the first column
other_columns = columns[1:]    # All columns except the first

# Find the midpoint of the remaining columns
midpoint = len(other_columns) // 2


for col in percentages:
    if col in df_transposed.columns:
        df_transposed[col] = df_transposed[col].apply(
            lambda x: float(x.replace('%', '')) / 100 if isinstance(x, str) and '%' in x else np.nan
        )


# Split into two DataFrames
df1 = df_transposed[first_column + other_columns[:midpoint]]  # First half + first column
df2 = df_transposed[first_column + other_columns[midpoint:]]  # Second half + first column


write_df_tblName('Screener_TBL1', df1)
write_df_tblName('Screener_TBL2', df2)


print("---------- Finished: other/stock_screeners_tbl.py ----------\n\n\n")