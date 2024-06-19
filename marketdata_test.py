import requests
import pandas as pd
<<<<<<< HEAD
from datetime import datetime

def convert_unix_to_datetime(unix_time):
    """Convert Unix timestamp to a human-readable datetime format."""
    if unix_time:
        try:
            # Try to convert assuming the timestamp is in seconds
            dt = datetime.utcfromtimestamp(unix_time)
            if dt.year < 1970 or dt.year > 2038:  # Plausibility check
                raise ValueError("Timestamp out of range, likely not in seconds.")
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (OSError, ValueError):
            pass

        try:
            # Try to convert assuming the timestamp is in milliseconds
            dt = datetime.utcfromtimestamp(unix_time / 1000.0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (OSError, ValueError):
            return None
    return None

def get_options_chain(ticker, expiration_date, api_key, request_counter):
    url = f"https://api.marketdata.app/v1/options/chain/{ticker}"
    params = {
        'expiration': expiration_date,
        'range': 'itm'  # Filter for in-the-money options
=======

def get_options_chain(ticker, expiration_date, api_key):
    url = f"https://api.marketdata.app/v1/options/chain/{ticker}"
    params = {
        'expiration': expiration_date,
>>>>>>> 63321d7 (	modified:   marketdata_test.py)
    }
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
<<<<<<< HEAD

    response = requests.get(url, params=params, headers=headers)
    request_counter['count'] += 1
    print(f"Fetching data for {ticker} with expiration {expiration_date}: Status Code {response.status_code}")

    if response.status_code in [200, 203]:
        try:
            data = response.json()
            print(f"Response JSON for {ticker} with expiration {expiration_date}: {data}")
        except ValueError:
            print(f"Failed to parse JSON for {ticker} with expiration {expiration_date}")
            return pd.DataFrame(), request_counter

        if not data:
            print(f"No data in response for {ticker} with expiration {expiration_date}")
            return pd.DataFrame(), request_counter

        options_data = []
        for option in data:
            if isinstance(option, dict):
                options_data.append({
                    "Updated Date": convert_unix_to_datetime(option.get('updated_at')),
                    "Expiration Date": convert_unix_to_datetime(option.get('expiration')),
                    "Ticker": option.get('ticker'),
                    "Strike": option.get('strike'),
                    "Last": option.get('last'),
                    "Theor.": option.get('theoretical'),
                    "IV": option.get('iv'),
                    "Delta": option.get('delta'),
                    "Gamma": option.get('gamma'),
                    "Theta": option.get('theta'),
                    "Vega": option.get('vega'),
                    "Rho": option.get('rho'),
                    "Volume": option.get('volume'),
                    "Open Int": option.get('open_interest'),
                    "Vol/OI": option.get('volume') / option.get('open_interest') if option.get('open_interest') else None,
                    "Type": option.get('type'),
                    "Last Trade": option.get('last_trade_date'),
                    "Avg IV": option.get('average_iv')
                })
            else:
                print(f"Unexpected data format: {option}")
        return pd.DataFrame(options_data), request_counter

    print(f"Response content for {ticker} with expiration {expiration_date}: {response.content}")

    return pd.DataFrame(), request_counter

# List of tickers and expiration dates
tickers = ['spy']
expiration_dates = ['2024-06-21']
=======
    
    response = requests.get(url, params=params, headers=headers)
    print(f"Fetching data for {ticker} with expiration {expiration_date}: Status Code {response.status_code}")

    if response.status_code != 200:
        print(f"Error fetching data for {ticker} with expiration {expiration_date}: {response.text}")
        return pd.DataFrame()

    data = response.json()
    
    if not data.get('data'):
        print(f"No data returned for {ticker} with expiration {expiration_date}")
        return pd.DataFrame()  # Return an empty DataFrame if no data is returned
    
    options_data = []
    for option in data['data']:
        options_data.append({
            "Updated Date": option.get('updated_at'),
            "Expiration Date": option.get('expiration'),
            "Ticker": option.get('ticker'),
            "Strike": option.get('strike'),
            "Last": option.get('last'),
            "Theor.": option.get('theoretical'),
            "IV": option.get('iv'),
            "Delta": option.get('delta'),
            "Gamma": option.get('gamma'),
            "Theta": option.get('theta'),
            "Vega": option.get('vega'),
            "Rho": option.get('rho'),
            "Volume": option.get('volume'),
            "Open Int": option.get('open_interest'),
            "Vol/OI": option.get('volume') / option.get('open_interest') if option.get('open_interest') else None,
            "Type": option.get('type'),
            "Last Trade": option.get('last_trade_date'),
            "Avg IV": option.get('average_iv')
        })
    
    return pd.DataFrame(options_data)

# List of tickers and expiration dates
tickers = ['spy', 'qqq', 'iwm', 'gld']
expiration_dates = ['2024-06-14', '2024-06-21', '2024-06-28']
>>>>>>> 63321d7 (	modified:   marketdata_test.py)

# Your MarketData API key
api_key = 'S0k2QXNsMFpEVlZZWXFlOXlEajJYcWlwZFA3XzRnYVJKVTVyZlFMbS1mUT0'

# Initialize an empty DataFrame to store all the data
all_data = pd.DataFrame()

<<<<<<< HEAD
# Initialize request counter
request_counter = {'count': 0}
max_requests = 100

# Loop through each ticker and expiration date
for ticker in tickers:
    for expiration_date in expiration_dates:
        if request_counter['count'] >= max_requests:
            print("Reached the maximum request limit.")
            break
        df, request_counter = get_options_chain(ticker, expiration_date, api_key, request_counter)
=======
# Loop through each ticker and expiration date
for ticker in tickers:
    for expiration_date in expiration_dates:
        df = get_options_chain(ticker, expiration_date, api_key)
>>>>>>> 63321d7 (	modified:   marketdata_test.py)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"No data for {ticker} with expiration {expiration_date}")
<<<<<<< HEAD
    if request_counter['count'] >= max_requests:
        break
=======
>>>>>>> 63321d7 (	modified:   marketdata_test.py)

# Save the combined data to a CSV file
if not all_data.empty:
    all_data.to_csv('options_chain_data.csv', index=False)
<<<<<<< HEAD
    print("Data saved to options_chain_data.csv")
else:
    print("No data to save")
=======
else:
    print("Data saved to options_chain_data.csv")
>>>>>>> 63321d7 (	modified:   marketdata_test.py)
