import requests
import pandas as pd
from datetime import datetime


def convert_unix_to_datetime(unix_time):
    """Convert Unix timestamp to a human-readable datetime format."""
    if unix_time:
        # Try to convert assuming the timestamp is in seconds
        try:
            dt = datetime.utcfromtimestamp(unix_time)
            if dt.year < 1970 or dt.year > 2038:  # Plausibility check
                raise ValueError("Timestamp out of range, likely not in seconds.")
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (OSError, ValueError):
            pass

        # Try to convert assuming the timestamp is in milliseconds
        try:
            dt = datetime.utcfromtimestamp(unix_time / 1000.0)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except (OSError, ValueError):
            return None
    return None

def get_options_chain(ticker, expiration_date, api_key, request_counter):
    url = f"https://api.marketdata.app/v1/options/chain/{ticker}"
    params = {
        'expiration': expiration_date,
        #'feed': 'cached',   # Use cached data to minimize credit consumption
        'range': 'itm'      # Filter for in-the-money options
    }
    headers = {
        'Authorization': f'Bearer {api_key}'
    }

    response = requests.get(url, params=params, headers=headers)
    request_counter['count'] += 1
    print(f"Fetching data for {ticker} with expiration {expiration_date}: Status Code {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        if data:
            #print("Data Returned:", data) 
            return pd.DataFrame(data), request_counter
        else:
            print(f"No data returned for {ticker} with expiration {params['expiration']}")

    if response.status_code == 203:
        data = response.json()
        if data:
            #print("Data Returned:", data) 
            return pd.DataFrame(data), request_counter
        else:
            print(f"No data returned for {ticker} with expiration {params['expiration']}")

    # Print the response content for inspection
    print(f"Response content for {ticker} with expiration {expiration_date}: {response.content}")

    data = response.json()

    if not data.get('data'):
        print(f"No data returned for {ticker} with expiration {expiration_date}")
        return pd.DataFrame(), request_counter  # Return an empty DataFrame if no data is returned

    options_data = []
    for option in data['data']:
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

    return pd.DataFrame(options_data), request_counter

# List of tickers and expiration dates
#tickers = ['spy', 'qqq', 'iwm', 'gld', 'appl', 'meta', 'msft', 'enph']
tickers = ['spy']
#expiration_dates = ['2024-06-14', '2024-06-21', '2024-06-28']
expiration_dates = ['2024-06-21']

# Your MarketData API key
api_key = 'S0k2QXNsMFpEVlZZWXFlOXlEajJYcWlwZFA3XzRnYVJKVTVyZlFMbS1mUT0'

# Initialize an empty DataFrame to store all the data
all_data = pd.DataFrame()

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
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"No data for {ticker} with expiration {expiration_date}")
    if request_counter['count'] >= max_requests:
        break

# Save the combined data to a CSV file
if not all_data.empty:
    all_data.to_csv('options_chain_data.csv', index=False)
    print("Data saved to options_chain_data.csv")
else:
    print("No data to save")
