#!/usr/bin/env python
# coding: utf-8

# # Creating a Data Extraction pipeline using Python

# Now its time to create a robust extraction pipeline using Python using the examples in previous chapter.
# Below is an example on how we can create an enterprise level pipeline.


# Make sure below packages have been installed 
# pip install pyarrow
# pip install certifi

import certifi
import json
import pandas as pd
import urllib3
from urllib3 import request
from sqlalchemy import create_engine


def source_data_from_parquet(parquet_file_name):
    try:
        df_parquet = pd.read_parquet(parquet_file_name)
    except Exception as e:
        df_parquet = pd.DataFrame()
    return df_parquet


def source_data_from_csv(csv_file_name):
    try:
        df_csv = pd.read_csv(csv_file_name)
    except Exception as e:
        df_csv = pd.DataFrame()
    return df_csv

def source_data_from_api(api_endpoint):
    try:
        # Check if API is available to retrive the data
        # Sometimes we get certificate error . We should never silence this error as this may cause a securirty threat.
        # Create a Pool manager that can be used to read the API response 
        http = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',ca_certs=certifi.where())
        api_response = http.request('GET', api_endpoint)
        apt_status = api_response.status
        if apt_status == 200:
            data = json.loads(api_response.data.decode('utf-8'))
            df_api = pd.json_normalize(data)
        else:
            df_api = pd.Dataframe()
    except Exception as e:
        df_api = pd.DataFrame()
    return df_api

def source_data_from_sql(sqlalchemy_engine, query):
    try:
        engine = create_engine(sqlalchemy_engine)
        with engine.connect() as conn:
            df_table = pd.read_sql(query, conn)

        # # Read sqlite query results into a pandas DataFrame
        # with sqlite3.connect(db_name) as conn:
        #     df_table = pd.read_sql(f"SELECT * from {table_name}", conn)
    except Exception as e:
        df_table = pd.DataFrame()
    return df_table


def source_data_from_webpage(web_page_url, matching_keyword):
    try:
        # Read webpage table into a pandas DataFrame
        df_html = pd.read_html(web_page_url, match = matching_keyword)
        df_html = df_html[0]
    except Exception as e:
        df_html = pd.DataFrame()
    return df_html

def extract_data():
        parquet_file_name = "../data/yellow_tripdata_2022-01.parquet"
        csv_file_name = "../data/h9gi-nx95.csv"
        api_endpoint = "https://data.cityofnewyork.us/resource/h9gi-nx95.json?$limit=500"
        sqlalchemy_engine = "mssql+pyodbc://datadevjobs.d.db.vu.local/DataDevJobsRepo?driver=ODBC+Driver+17+for+SQL+Server"
        query = "SELECT * FROM Logging.SolutionOverviewServers"
        web_page_url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
        matching_keyword = "by country"

        # Extract data from all source systems
        # Now these dataframes are available for loading data into either VSA table, PSA table or to be consumed in
        # transformation pipeline.

        # # this approach uses tuple assignment which is a pythonic way of assigning multiple variables at once,
        # # and they run asynchronously
        # df_parquet,df_csv,df_api,df_table,df_html = (source_data_from_parquet(parquet_file_name),
        #                                              source_data_from_csv(csv_file_name),
        #                                              source_data_from_api(api_endpoint),
        #                                              source_data_from_sql(sqlalchemy_engine, query),
        #                                              source_data_from_webpage(web_page_url,matching_keyword))
        # return df_parquet,df_csv,df_api,df_table,df_html

        # this approach runs them one at a time for easier debugging
        # Retrieve data from a Parquet file
        df_parquet = source_data_from_parquet(parquet_file_name)

        # Retrieve data from a CSV file
        df_csv = source_data_from_csv(csv_file_name)

        # Fetch data from an API endpoint
        df_api = source_data_from_api(api_endpoint)

        # Execute a SQL query to retrieve data from a database
        df_table = source_data_from_sql(sqlalchemy_engine, query)

        # Scrape data from a webpage
        df_html = source_data_from_webpage(web_page_url, matching_keyword)

        # Return the DataFrames
        return df_parquet, df_csv, df_api, df_table, df_html

# #run main extract_data() function and print sample output
# df_parquet, df_csv, df_api, df_table, df_html = extract_data()
#
# print(df_parquet.head())
# print(df_csv.head())
# print(df_api.head())
# print(df_table.head())
# print(df_html.head())
