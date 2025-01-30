from sqlalchemy import create_engine, text
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv

load_dotenv()

def get_df_tblName(table):
    port = int(os.getenv("mysql_port"))
    user = os.getenv("mysql_user")
    password = os.getenv("mysql_password")
    host = os.getenv("mysql_host")
    database = os.getenv("mysql_database")

    try:
        # Create a connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Query the table and load into DataFrame
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, engine)

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()


def get_df_query(query):
    port = int(os.getenv("mysql_port"))
    user = os.getenv("mysql_user")
    password = os.getenv("mysql_password")
    host = os.getenv("mysql_host")
    database = os.getenv("mysql_database")

    try:
        # Create a connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Execute the query and load into DataFrame
        df = pd.read_sql(query, engine)

        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def get_cursor(query, params=None):
    port = int(os.getenv("mysql_port"))
    user = os.getenv("mysql_user")
    password = os.getenv("mysql_password")
    host = os.getenv("mysql_host")
    database = os.getenv("mysql_database")

    try:
        # Create a connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Connect to the database and execute the query with parameters
        with engine.connect() as connection:
            result = connection.execute(text(query), params).fetchone()  # Fetch the first result

        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def fetch_tables_for_screener():
    port = int(os.getenv("mysql_port"))
    user = os.getenv("mysql_user")
    password = os.getenv("mysql_password")
    host = os.getenv("mysql_host")
    database = os.getenv("mysql_database")
    try:
        # Create the database URL for SQLAlchemy
        database_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

        # Create SQLAlchemy engine
        engine = create_engine(database_url)

        # List of patterns to match in table names
        patterns = [
            "annual_income_statement",
            "annual_balance_sheet",
            "annual_cash_flow_statement",
            "annual_sup_IS",
            "annual_sup_BS",
            "annual_sup_CF",
            "annual_key_ratios"
        ]

        # Create a query with OR conditions for each pattern
        like_conditions = " OR ".join([f"Tables_in_{database} LIKE '%{pattern}%'" for pattern in patterns])
        query = f"SHOW TABLES WHERE {like_conditions}"

        # Execute the query
        with engine.connect() as conn:
            result = conn.execute(text(query))

            # Fetch all matching tables
            tables = result.fetchall()
        tables = [table[0] for table in tables]
        return tables

    except Exception as e:
        print(f"Error: {e}")
        return []


def write_df_tblName(table_name,df):
    port = int(os.getenv("mysql_port"))
    user = os.getenv("mysql_user")
    password = os.getenv("mysql_password")
    host = os.getenv("mysql_host")
    database = os.getenv("mysql_database")

    try:
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

        # Write the DataFrame to the specified MySQL table
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"DataFrame successfully written to table '{table_name}'.")
    except SQLAlchemyError as e:
        print(f"An error occurred: {e}")
    finally:
        engine.dispose()