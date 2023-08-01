#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import json
from dotenv import load_dotenv
import logging
logging.basicConfig(level=logging.INFO)

import requests
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 50)

from google.oauth2 import service_account
from google.cloud import firestore
from google.cloud import bigquery
from pandas_gbq import gbq

from datetime import datetime as dt
from datetime import timedelta 

from utils import FlightAwareAPI
from utils import JSON_EncoderDecoder


# In[2]:


## I/O Functions

def get_flight_data(api, identifier, start_datetime, end_datetime, max_pages=40):
    """
    Get flight data from FlightAware API, given an identifier and a time range.
    """
    endpoint_base = '/flights/'
    endpoint = f'{endpoint_base}{identifier}'
    try:
        data = api.query(endpoint, 
                end= end_datetime, 
                start= start_datetime, 
                max_pages=max_pages)

    except requests.HTTPError as e:
        print(e)

    df = pd.json_normalize(data, 'flights')

    return df

def get_last_run_timestamp(identifier, firestore_client):
    doc_ref = firestore_client.collection('flight_timestamps').document(identifier)
    doc = doc_ref.get()
    if doc.exists:
        return pd.Timestamp(doc.get('last_run_ts'))
    else:
        print('No existing timestamp, returning early timestamp.')
        return pd.Timestamp('2000-01-01 00:00:00')

def update_last_run_timestamp(identifier, timestamp, firestore_client):
    doc_ref = firestore_client.collection('flight_timestamps').document(identifier)
    doc_ref.set({'last_run_ts': timestamp})


# def get_last_run_timestamp(table_ref, client):
#     # Define the query to get the max timestamp
#     query = f"SELECT MAX(crt_ts) as max_ts FROM `{table_ref}`"


#     # May refactor this to distingish between a non-existen or empty table and a query error.
#     # This timestamp is crucial to data quality.
#     try:        
#         df_run_ts = client.query(query).to_dataframe()  # API request
#         last_run_ts = df_run_ts['max_ts'][0]
        
#         # This will raise an error if the timestamp is NaT, or Null which occurs when the table is empty.

#         assert type(last_run_ts) == pd._libs.tslibs.timestamps.Timestamp

#         return last_run_ts
    
#     except Exception as e:
#         # If an error occurs (such as the table not existing), return a very early timestamp
#         print('No existing table, returning early timestamp.')
#         return pd.Timestamp('2000-01-01 00:00:00')


# In[3]:


## Transformation Functions

def rename_columns_remove_periods(dataframe):
    new_columns = dataframe.columns.str.replace(".", "_")
    dataframe = dataframe.rename(columns=dict(zip(dataframe.columns, new_columns)))
    return dataframe

def create_crt_ts_cols(df):
    df['crt_ts'] = pd.to_datetime('now') 
    df['crt_ts_year'] = df['crt_ts'].dt.year
    df['crt_ts_month'] = df['crt_ts'].dt.month
    df['crt_ts_day'] = df['crt_ts'].dt.day
    df['crt_ts_hour'] = df['crt_ts'].dt.hour
    return df

def datatype_cleanup(df):
    df['codeshares'] = df['codeshares'].astype(str)
    df['codeshares_iata'] = df['codeshares_iata'].astype(str)
    return df


# In[9]:


def main(identifier):

    # Environment variables
    load_dotenv()  # take environment variables from .env.
    
    # GCP Serive Account Credentials
    gcp_creds_encoded = os.environ.get('GCP_CREDENTIALS_JSON_ENCODED')
    # See Utils.py for the JSON_EncoderDecoder class
    gcp_credentials_json = JSON_EncoderDecoder(gcp_creds_encoded).decode().get()
    gcp_credentials = service_account.Credentials.from_service_account_info(gcp_credentials_json)
    
    # FlightAware API Key
    fa_api_key = os.getenv('FLIGHTAWARE_API_KEY')



    # API QUERY PARAMETERS
    # identifier = 'AAL2563'

    lookback_hours = 7*24 # based on the flight's estimated or actual departure
    lookfoward_hours = 2*24 
    # Timestamp calculations
    query_start = (dt.now()- timedelta(hours=lookback_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
    current_time = dt.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    query_end = (dt.now()+ timedelta(hours=lookfoward_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')


    # BIGQUERY OUTPUT PARAMETERS
    project_id = 'aia-ds-accelerator-flight-1'
    dataset_id = 'flightaware'
    table_id = 'flightsummary_raw'
    # BigQuery output table name
    table_ref_out = f"{project_id}.{dataset_id}.{table_id}"

    client = bigquery.Client(credentials=gcp_credentials, project=project_id)
    firestore_client = firestore.Client(credentials=gcp_credentials, project=project_id)


    # ------ EXECUTION ------
    API = FlightAwareAPI(fa_api_key)
    df = get_flight_data(API, identifier, query_start, query_end)

    # Rename columns with '.' in the name.
    df = rename_columns_remove_periods(df)

    # Convert columns to string to avoid errors when writing to BigQuery
    df = datatype_cleanup(df)


    # Get the last run timestamp from Firestore
    last_run_ts = get_last_run_timestamp(identifier, firestore_client)
    # last_run_ts = get_last_run_timestamp(table_ref=table_ref_out, client=client)

    df['last_run_ts'] = last_run_ts
    print(f'last run timestamp: {last_run_ts}')

    # Add current run timestamp column
    df = create_crt_ts_cols(df)

    
    # Write to BigQuery
    try:
        # Append the data to the table. If the table doesn't exist, create it.
        gbq.to_gbq(df, table_ref_out, project_id=project_id, if_exists='append', credentials=gcp_credentials)
        logging.info(f"Data loaded successfully to {table_ref_out}")
    except gbq.GenericGBQException as e:
        logging.error(f"An error occurred: {e}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    
    # Update the last run timestamp in Firestore
    update_last_run_timestamp(identifier, dt.now().strftime('%Y-%m-%dT%H:%M:%SZ'), firestore_client)



# In[10]:


# Performing the execution in here prevents main() from being called when the module is imported
# and it allows us to run this file directly from the command line

if __name__ == "__main__":
    main(identifier='AAL2563')

