import requests
import pandas as pd

def get_options_chain(ticker, expiration_date, api_key):
    url = f"https://api.marketdata.app/v1/options/chain/{ticker}"
    params = {
        'expiration': expiration_date,
    }
    headers = {
        'Authorization': f'Bearer {api_key}'
    }
    
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

# Your MarketData API key
api_key = 'S0k2QXNsMFpEVlZZWXFlOXlEajJYcWlwZFA3XzRnYVJKVTVyZlFMbS1mUT0'

# Initialize an empty DataFrame to store all the data
all_data = pd.DataFrame()

# Loop through each ticker and expiration date
for ticker in tickers:
    for expiration_date in expiration_dates:
        df = get_options_chain(ticker, expiration_date, api_key)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"No data for {ticker} with expiration {expiration_date}")

# Save the combined data to a CSV file
if not all_data.empty:
    all_data.to_csv('options_chain_data.csv', index=False)
else:
    print("Data saved to options_chain_data.csv")
