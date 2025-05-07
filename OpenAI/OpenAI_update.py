import sys
import os
parent_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(1, parent_path)  # caution: path[0] is reserved for script path (or '' in REPL)

from utils.OpenAI_functions import run_assistant, get_files, clear_all, run_model_over_df
from utils.mysql_connect_funcs import get_df_tblName, insert_row_SC, replace_row
import os
import pandas as pd

print("---------- Starting: OpenAI/OpenAI_update.py ----------")

#docs_path = "../annual_reports/"
tickers = [folder for folder in os.listdir(os.path.join(parent_path, "annual_reports")) if os.path.isdir(os.path.join(parent_path, "annual_reports", folder))]

if not tickers:
    print('No updates')
else:
    df_metadata = get_df_tblName('metadataTBL')

    ####### history code #######
    assistantID = "asst_JPa6tNP3ONOV40z3PgSEUz63"
    rows=[]
    for i in range(len(tickers)):
        try:
            filtered_df = df_metadata[df_metadata['symbol'] == tickers[i]].reset_index(drop=True)
            company_name = filtered_df['name'].iloc[0]
            file_paths, years = get_files(os.path.join(parent_path, "annual_reports"), tickers[i])
            for j in range(len(file_paths)):
                try:
                    prompt = "Summarise the all the updates and changes that happened to " + tickers[i] + '(' + company_name + ')' + " in " + years[j]
                    message = run_assistant(file_paths[j], prompt, assistantID)
                    ID = tickers[i] + '_' + years[j]
                    row = [ID, tickers[i], years[j], message]
                    rows.append(row)
                except Exception as e:
                    print(f"{tickers[i]} {years[j]} failed:{e}")
        except Exception as e:
            print(f"{tickers[i]} failed:{e}")

    df_history = pd.DataFrame(rows, columns=['ID', 'ticker', 'year', 'content'])

    prompt = "The text below is an answer given to the question 'What happened in to the company this year'. Please replace any text that suggest that an answer could not be found and any requests for additional information with 'No information is available'. Then reword it to remove any conversational elements from the text or anything tht requests for additional input:\n"
    system = "You are a helpful assistant"
    df_cleanLong = df_history.copy()
    df_cleanLong = run_model_over_df(df_cleanLong,60,prompt,system)

    prompt = "Please summarise this text below into a couple of dot points. Don't make any headings:\n"
    system = "You are an editing assistant. Your job is to make summaries of the input text."
    df_short = df_cleanLong.copy()
    df_short = run_model_over_df(df_short,60,prompt,system)

    for index, row in df_history.iterrows():
        data = row.tolist()
        insert_row_SC('History', data, ['ID', 'ticker', 'year', 'content'])

    for index, row in df_cleanLong.iterrows():
        data = row.tolist()
        insert_row_SC('History_long', data, ['ID', 'ticker', 'year', 'content'])

    for index, row in df_short.iterrows():
        data = row.tolist()
        insert_row_SC('History_short', data, ['ID', 'ticker', 'year', 'content'])

    ####### segment code, indiv results for each year is in a different script #######
    assistantID = 'asst_G7AiFsK5wxCbMRwOxizt4T2T'
    rows=[]
    for i in range(len(tickers)):
        try:
            file_paths, years = get_files(os.path.join(parent_path, "annual_reports"), tickers[i])
            for j in range(len(file_paths)):
                try:
                    prompt = "please extract the operating results for all the company's business segments"
                    message = run_assistant(file_paths[j], prompt, assistantID)
                    ID = tickers[i] + '_' + years[j]
                    row = [ID, tickers[i], years[j], message]
                    rows.append(row)
                except Exception as e:
                    print(f"{tickers[i]} {years[j]} failed:{e}")
        except Exception as e:
            print(f"{tickers[i]} failed:{e}")

    df_segmentResults = pd.DataFrame(rows, columns=['ID', 'ticker', 'year', 'content'])

    for index, row in df_segmentResults.iterrows():
        data = row.tolist()
        insert_row_SC('segmentResultsExtract', data, ['ID', 'ticker', 'year', 'content'])

    ####### business profile code #######
    assistantID = 'asst_6xnQG5tz74YSLYBmpoZUWRAa'
    rows=[]
    for i in range(len(tickers)):
        try:
            file_paths, years = get_files(os.path.join(parent_path, "annual_reports"), tickers[i])
            for j in range(len(file_paths)):
                try:
                    prompt = "Please go through the attached document and write me a detailed report on what products/services the company sells, products/services that are in production, what are the business segments and how much of the company are they, what the business model is, who are the company's customers, who are the company's suppliers if there are any, what geographical regions the company operates in and what is there exposure to these regions, what market forces are affecting the business, what are the comapny's growth drivers and what are the key business risks the comapny faces. \n"
                    message = run_assistant(file_paths[j], prompt, assistantID)
                    row = [tickers[i], message]
                    rows.append(row)
                except Exception as e:
                    print(f"{tickers[i]} {years[j]} failed:{e}")
        except Exception as e:
            print(f"{tickers[i]} failed:{e}")

    df_companyDetails = pd.DataFrame(rows, columns=['ticker', 'content'])


    prompt = "The text below is an answer given to the question 'What does the company do'. Please replace any text that suggest that an answer could not be found and any requests for additional information with 'No information is available'. Then remove any text before the headings. Then reword it to remove any conversational elements from the text or anything tht requests for additional input:\n"
    system = "You are a helpful assistant"
    df_companyDetails = run_model_over_df(df_companyDetails,60,prompt,system)

    for index, row in df_companyDetails.iterrows():
        data = row.tolist()
        replace_row('companyDetails', data, ['ticker', 'content'])

    clear_all()

print("---------- Finished: OpenAI/OpenAI_update.py ----------\n\n\n")