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
    response = rq.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Failed to fetch data, check status code {response.status_code}")
        return None

        # Valid values for results per page is between 1-250


exchange_params = {
            "per_page": 250,
            "page": 1
}

PUB_URL = "https://api.coingecko.com/api/v3"
exchange_list_response = get_response("/exchanges", use_demo, exchange_params, PUB_URL)
if exchange_list_response:
    df_ex = pd.DataFrame(exchange_list_response)
else:
    df_ex = pd.DataFrame()

df_ex_subset = df_ex[["id", "name", "country", "trade_volume_24h_btc"]]
df_ex_subset = df_ex_subset.sort_values(by = ["trade_volume_24h_btc"], ascending = False)

def get_trade_exchange(id, base_curr, target_curr):
    
    exchange_ticker_response = get_response(f"/exchanges/{id}/tickers",
                                            use_demo,
                                            {},
                                            PUB_URL)
    if not exchange_ticker_response:
        return ""
    
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

def get_trade_exchange_per_country(country, base_curr, target_curr):
    df_all = df_ex_subset[df_ex_subset["country"] == country]    
    
    if df_all.empty:
        print(f"No exchanges found for country: {country}")
        return pd.DataFrame(columns=['exchange', 'last_price', 'last_vol', 'spread', 'trade_time'])
    
    exchanges_list = df_all["id"]
    ex_all = []    
    
    print(f"Found {len(exchanges_list)} exchanges in {country}")
       
    for exchange_id in exchanges_list:
        found_match = get_trade_exchange(exchange_id, base_curr, target_curr)
        if found_match == "":
            continue
        else:
            try:
                temp_dict = dict(
                                exchange = exchange_id,
                                last_price = found_match["last"],
                                last_vol   = found_match["volume"],
                                spread     = found_match["bid_ask_spread_percentage"],
                                trade_time = convert_to_local_tz(found_match["last_traded_at"])
                                )
                ex_all.append(temp_dict)
                print(f"Added data for exchange: {exchange_id}")
            except KeyError as e:
                print(f"Missing data in response for exchange {exchange_id}: {e}")
                continue
    
    # Convert to DataFrame before returning
    df_result = pd.DataFrame(ex_all)
    if df_result.empty:
        print(f"No trading data found for {base_curr}/{target_curr} in {country}")
        return pd.DataFrame(columns=['exchange', 'last_price', 'last_vol', 'spread', 'trade_time'])
    return df_result

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

def display_agg_per_exchange(df_ex_all, base_curr):
    if df_ex_all.empty:
        print("No data to display")
        return None
    
    required_columns = ['exchange', 'last_price', 'last_vol', 'spread', 'trade_time']
    if not all(col in df_ex_all.columns for col in required_columns):
        print(f"Missing required columns. Available columns: {df_ex_all.columns.tolist()}")
        return None
    
    # Group data and calculate statistics per exchange    
    df_agg = (
        df_ex_all.groupby("exchange").agg
        (        
            trade_time_min = ("trade_time", 'min'),
            trade_time_latest = ("trade_time", 'max'),
            last_price_mean = ("last_price", 'mean'),
            last_vol_mean = ("last_vol", 'mean'),
            spread_mean = ("spread", 'mean'),
            num_trades = ("last_price", 'count')
        )
    )
    
    # Get time interval over which statistics have been calculated    
    df_agg["trade_time_duration"] = df_agg["trade_time_latest"] - df_agg["trade_time_min"]
    
    # Reset columns so that we can access exchanges below
    df_agg = df_agg.reset_index()
    
    # Calculate % of total volume for all exchanges
    last_vol_pert = []
    for i, row in df_agg.iterrows():
        try:
            df_vol = get_vol_exchange(row["exchange"], 30, base_curr)
            current_vol = df_vol["volume_SMA"][0]
            vol_pert = (row["last_vol_mean"] / current_vol) * 100
            last_vol_pert.append(vol_pert)
        except:
            last_vol_pert.append("")
            continue
            
    # Add % of total volume column
    df_agg["last_vol_pert"] = last_vol_pert
    
    # Remove redundant column
    df_agg = df_agg.drop(columns = ["trade_time_min"])
    
    # Round all float values
    # (seems to be overwritten by style below)
    df_agg = df_agg.round({"last_price_mean": 2,
                           "last_vol_mean": 2,
                           "spread_mean": 2
                          })
    
    display(df_agg.style.apply(highlight_max_min,
                               color = 'green',
                               subset = "last_price_mean")
           )
           
    return None

def highlight_max_min(x, color):
    
    return np.where((x == np.nanmax(x.to_numpy())) |
                    (x == np.nanmin(x.to_numpy())),
                    f"color: {color};",
                    None)

def run_bot(country,
            base_curr,
            target_curr):
    print(f"Starting bot for {country} - {base_curr}/{target_curr}")
    
    df_ex_all = get_trade_exchange_per_country(country, base_curr, target_curr)
    if df_ex_all.empty:
        print("No initial data found. Checking again in 60 seconds...")
    
    # Collect data every minute    
    while True:
        time.sleep(60)
        df_new = get_trade_exchange_per_country(country, base_curr, target_curr)
        
        if not df_new.empty:
            # Merge to existing DataFrame
            df_ex_all = pd.concat([df_ex_all, df_new])
            
            # Remove duplicate rows based on all columns
            df_ex_all = df_ex_all.drop_duplicates()
            
            # Clear previous display once new one is available
            clear_output(wait = True)
            display_agg_per_exchange(df_ex_all, base_curr)
        else:
            print("No new data found in this iteration")
        
    return None

def get_top_coins():
    """Get top 100 coins by market cap"""
    coins_params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1
    }
    
    coins_response = get_response("/coins/markets", use_demo, coins_params, PUB_URL)
    return [(coin['symbol'].upper(), coin['name']) for coin in coins_response]

if __name__ == "__main__":
    # Get top 10 exchanges by volume
    print("Fetching top 10 exchanges...")
    top_exchanges = df_ex_subset.head(10)
    print(f"Top 10 exchanges:\n{top_exchanges[['name', 'country', 'trade_volume_24h_btc']]}\n")

    # Get top 100 coins
    print("Fetching top 100 coins...")
    top_coins = get_top_coins()
    print(f"Retrieved {len(top_coins)} coins\n")

    # Initialize results storage
    exchange_data = []

    # Iterate through top exchanges and coins
    for _, exchange in top_exchanges.iterrows():
        print(f"\nChecking exchange: {exchange['name']}")
        
        for base_symbol, base_name in top_coins[:10]:  # Start with top 10 coins for testing
            print(f"Checking {base_name} ({base_symbol}) pairs...")
            
            # Check against USDT and BTC pairs
            for target in ['USDT', 'BTC']:
                try:
                    trade_data = get_trade_exchange(exchange['id'], base_symbol, target)
                    if trade_data:
                        print(f"Found {base_symbol}/{target} pair!")
                        exchange_data.append({
                            'exchange': exchange['name'],
                            'pair': f"{base_symbol}/{target}",
                            'price': trade_data['last'],
                            'volume': trade_data['volume'],
                            'spread': trade_data['bid_ask_spread_percentage']
                        })
                except Exception as e:
                    print(f"Error checking {base_symbol}/{target}: {str(e)}")

    # Convert results to DataFrame and display
    if exchange_data:
        results_df = pd.DataFrame(exchange_data)
        print("\nFinal Results:")
        print(results_df.sort_values('volume', ascending=False))
    else:
        print("\nNo trading pairs found!")

    # Example of monitoring specific pairs
    print("\nStarting continuous monitoring for top exchanges...")
    try:
        # Monitor US exchanges for BTC/USDT
        run_bot("US", "BTC", "USDT")
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user")

