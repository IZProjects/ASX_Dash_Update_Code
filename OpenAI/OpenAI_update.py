from utils.OpenAI_functions import run_assistant, get_files, clear_all, run_model_over_df
from utils.mysql_connect_funcs import get_df_tblName, insert_row_SC, replace_row
import os
import pandas as pd

docs_path = "../annual_reports/"
tickers = [folder for folder in os.listdir(docs_path) if os.path.isdir(os.path.join(docs_path, folder))]

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
            file_paths, years = get_files(docs_path, tickers[i])
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
            file_paths, years = get_files(docs_path, tickers[i])
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

    ####### segment description code #######
    assistantID = 'asst_itFTGt6IWtVjfqCBrdVMfLdk'
    rows=[]
    for i in range(len(tickers)):
        try:
            file_paths, years = get_files(docs_path, tickers[i])
            for j in range(len(file_paths)):
                try:
                    prompt = "Please get a description of each of the business segments in the company based on the attached file. \n"
                    message = run_assistant(file_paths[j], prompt, assistantID)
                    row = [tickers[i], message]
                    rows.append(row)
                except Exception as e:
                    print(f"{tickers[i]} {years[j]} failed:{e}")
        except Exception as e:
            print(f"{tickers[i]} failed:{e}")

    df_segmentDescription = pd.DataFrame(rows, columns=['ticker', 'content'])

    for index, row in df_segmentDescription.iterrows():
        data = row.tolist()
        replace_row('SegmentDescription', data, ['ticker', 'content'])



    clear_all()
