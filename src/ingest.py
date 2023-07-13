#!/usr/bin/env python
# coding: utf-8

# In[14]:


import os
import requests
import pandas as pd
import pyarrow as pa
import awswrangler as wr

from datetime import datetime as dt
from datetime import timedelta 

from utils import FlightAwareAPI


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

def create_crt_ts_cols(df):
    df['crt_ts'] = dt.now()#.strftime('%Y-%m-%dT%H:%M:%SZ')
    df['crt_ts_year'] = df['crt_ts'].dt.year
    df['crt_ts_month'] = df['crt_ts'].dt.month
    df['crt_ts_day'] = df['crt_ts'].dt.day
    df['crt_ts_hour'] = df['crt_ts'].dt.hour
    return df


# In[15]:


def main():
    # CREDENTIALS - DELETE THIS LINE BEFORE COMMITTING
    os.environ['FLIGHTAWARE_API_KEY'] = 'w4tsTSMNABOlC4HTtKsc38Odp8CDGAK9'
    os.environ["AWS_ACCESS_KEY_ID"] = "AKIASJUJRRTXE7L2J6FD"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "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"


    # QUERY PARAMETERS
    lookback_hours = 7*24 # based on the flight's estimated or actual departure
    lookfoward_hours = 2*24 
    identifier = 'AAL2563'

    # S3 PARAMETERS
    bucket = "flight-ml-demo"
    table_path = "ingested_raw/flightsummary" # since table is partitioned, does end with .parquet

    s3_path = f"{bucket}/{table_path}"
    s3_path_full = f"s3://{bucket}/{table_path}"


    # Timestamp calculations
    query_start = (dt.now()- timedelta(hours=lookback_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
    current_time = dt.now().strftime('%Y-%m-%dT%H:%M:%SZ')
    query_end = (dt.now()+ timedelta(hours=lookfoward_hours)).strftime('%Y-%m-%dT%H:%M:%SZ')
    

    # EXECUTION
    API = FlightAwareAPI(os.getenv('FLIGHTAWARE_API_KEY'))

    df = get_flight_data(API, identifier, query_start, query_end)
    # print('API call complete')

    df = create_crt_ts_cols(df)
    
    # print('data transformations complete')

    wr.s3.to_parquet(
        df=df,
        path=s3_path_full,
        dataset=True,
        mode="append",
        partition_cols=['crt_ts_year', 'crt_ts_month', 'crt_ts_day','ident']
    )
    print('Append to s3 complete at '+str(current_time))


# In[16]:


# Performing the execution in here prevents main() from being called when the module is imported
# and it allows us to run this file directly from the command line

if __name__ == "__main__":
    main()


# In[ ]:




