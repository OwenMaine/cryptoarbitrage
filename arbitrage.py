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

def get_api_key():
    try:
        with open('config.json') as f:
            config = json.load(f)
            return config['api_key']
    except FileNotFoundError:
        raise Exception("config.json file not found. Please create one with your API key.")

use_demo = {
    "accept": "application/json",
    "x-cg-demo-api-key": get_api_key()
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

df_ex_subset = df_ex[["id", "name", "country", "trade_volume_24h_btc"]]
df_ex_subset = df_ex_subset.sort_values(by = ["trade_volume_24h_btc"], ascending = False)

def get_trade_exchange(id, base_curr, target_curr):
    
    exchange_ticker_response = get_response(f"/exchanges/{id}/tickers",
                                            use_demo,
                                            {},
                                            PUB_URL)
    
    found_match = ""
    
    for ticker in exchange_ticker_response["tickers"]:
        if ticker["base"] == base_curr and ticker["target"] == target_curr:
            found_match = ticker
            break
            
    if found_match == "":
        warnings.warn(f"No data found for {base_curr}-{target_curr} pair in {id}")
    
    return found_match

    def convert_to_local_tz(old_ts):
        
        new_tz = pytz.timezone("Africa/Accra")
        old_tz = pytz.timezone("UTC")
        
        format = "%Y-%m-%dT%H:%M:%S+00:00"
        datetime_obj = datetime.datetime.strptime(old_ts, format)
        
        localized_ts = old_tz.localize(datetime_obj)
        new_ts = localized_ts.astimezone(new_tz)
        
        return new_ts

    def get_trade_exchange_per_country(country,
                                       base_curr,
                                       target_curr):
        
        df_all = df_ex_subset[(df_ex_subset["country"] == country)]    
        
        exchanges_list = df_all["id"]
        ex_all = []    
           
        for exchange_id in exchanges_list:
            found_match = get_trade_exchange(exchange_id, base_curr, target_curr)
            if found_match == "":
                continue
            else:
                temp_dict = dict(
                                 exchange = exchange_id,
                                 last_price = found_match["last"],
                                 last_vol   = found_match["volume"],
                                 spread     = found_match["bid_ask_spread_percentage"],
                                 trade_time = convert_to_local_tz(found_match["last_traded_at"])
                                 )
                ex_all.append(temp_dict)

    def get_exchange_rate(base_curr):
        
        # This returns current BTC to base_curr exchange rate    
        exchange_rate_response = get_response(f"/exchange_rates",
                                              use_demo,
                                              {},
                                              PUB_URL)
        rate = ""
        try:
            rate = exchange_rate_response["rates"][base_curr.lower()]["value"]
        except KeyError as ke:
            print("Currency not found in the exchange rate API response:", ke)
            
        return rate  

    def get_vol_exchange(id, days, base_curr):
        
        vol_params = {"days": days}
        
        exchange_vol_response = get_response(f"/exchanges/{id}/volume_chart",
                                             use_demo,
                                             vol_params,
                                             PUB_URL)
        
        time, volume = [], []
        
        # Get exchange rate when base_curr is not BTC
        ex_rate = 1.0
        if base_curr != "BTC":
            ex_rate = get_exchange_rate(base_curr)
            
            # Give a warning when exchange rate is not found
            if ex_rate == "":
                print(f"Unable to find exchange rate for {base_curr}, vol will be reported in BTC")
                ex_rate = 1.0
        
        for i in range(len(exchange_vol_response)):
            # Convert to seconds
            s = exchange_vol_response[i][0] / 1000
            time.append(datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d'))
            
            # Default unit for volume is BTC
            volume.append(float(exchange_vol_response[i][1]) * ex_rate)
                          
        df_vol = pd.DataFrame(list(zip(time, volume)), columns = ["date", "volume"])
        
        # Calculate SMA for a specific window
        df_vol["volume_SMA"] = df_vol["volume"].rolling(7).mean()
        
        return df_vol.sort_values(by = ["date"], ascending = False).reset_index(drop = True)