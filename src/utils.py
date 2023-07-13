import requests
import json
from datetime import datetime as dt
from typing import Dict, Any
from google.cloud import bigquery

class FlightAwareAPI:
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = 'https://aeroapi.flightaware.com/aeroapi'

    def _build_headers(self):
        return {
            'x-apikey': self.api_key,
        }

    def query(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        url = self.base_url + endpoint
        headers = self._build_headers()
        response = requests.get(url, headers=headers, params=kwargs)

        if response.status_code == 200:
            return response.json()
        else:
            raise requests.HTTPError(f"Error: {response.status_code}, {response.text}")
        


def run_bigquery_query(query):
    # Create a BigQuery client.
    client = bigquery.Client()

    # Run the query
    query_job = client.query(query)  # API request
    results_df = query_job.to_dataframe()  # Waits for query to finish and returns a DataFrame

    return results_df