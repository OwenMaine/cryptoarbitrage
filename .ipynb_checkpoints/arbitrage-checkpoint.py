import requests as rq
import json
import pandas as pd
pd.set_option('display.precision', 4,
'display.colheader_justify', 'center')
import numpy as np
import warnings
import pytz
import datetime
import time
from IPython.display import clear_output

# Remove the get_demo_key function as it's no longer needed

use_demo = {
    "accept": "application/json",
    "x-cg-demo-api-key": "CG-daTziMBfXSDTJEWxbGBZMvqR"
}

def get_response(endpoint, headers, params, URL):
    url = "".join((URL, endpoint))
    response = rq.get(url, headers = headers, params = params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data, check status code {response.status_code}")

        # Valid values for results per page is between 1-250
exchange_params = {
            "per_page": 250,
            "page": 1
}

PUB_URL = "https://api.coingecko.com/api/v3"
exchange_list_response = get_response("/exchanges", use_demo, exchange_params, PUB_URL)
df_ex = pd.DataFrame(exchange_list_response)
