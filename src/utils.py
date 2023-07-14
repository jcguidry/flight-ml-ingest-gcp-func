import requests
import json
from datetime import datetime as dt
from typing import Dict, Any

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
        



import base64

class JSON_EncoderDecoder():
    """
    GCP service account keys (for auth) are stored as JSON objects, loaded from a file.
    Thie allows you to avoid storing the key in a file.
    
    Encodes and decodes json objects to and from strings.
    This is useful for storing json objects in environment variables.

    """
    def __init__(self, json_object):
        self.json_object = json_object

    def encode(self):
        '''encodes json to a string which can be stored in 
        an environment variable'''
        assert isinstance(self.json_object, dict), 'Variable to encode must be a dict.'
        x = json.dumps(self.json_object)
        self.json_object = base64.b64encode(x.encode('utf-8'))
        return self
    
    def decode(self):
        '''decodes json from a string which can be stored in 
        an environment variable'''
        assert isinstance(self.json_object, str), 'Variable to decode must be a string.'
        x = str(self.json_object)[2:-1]
        self.json_object = json.loads(base64.b64decode(x).decode('utf-8'))
        return self
    
    def get(self):
        return self.json_object



