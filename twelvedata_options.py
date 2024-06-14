import requests
import pandas as pd

def get_current_price(ticker, api_key):
    url = f"https://api.twelvedata.com/price"
    params = {
        'symbol': ticker,
        'apikey': api_key
    }
    
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        price = data.get('price')
        if price is not None:
            return float(price)
        else:
            print(f"No price data returned for {ticker}")
            return None
    else:
        print(f"Error fetching current price for {ticker}: {response.status_code} - {response.text}")
        return None

def get_options_chain(ticker, expiration_date, api_key, price_range_pct=10):
    current_price = get_current_price(ticker, api_key)
    if current_price is None:
        return pd.DataFrame()
    
    lower_bound = current_price * (1 - price_range_pct / 100)
    upper_bound = current_price * (1 + price_range_pct / 100)
    
    url = f"https://api.twelvedata.com/options_chain"
    params = {
        'symbol': ticker,
        'expiration': expiration_date,
        'apikey': api_key
    }

    response = requests.get(url, params=params)
    print(f"Fetching data for {ticker} with expiration {expiration_date}: Status Code {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        if not data.get('options'):
            print(f"No data returned for {ticker} with expiration {expiration_date}")
            return pd.DataFrame()
        
        options_data = []
        for option in data['options']:
            strike_price = option.get('strike')
            if lower_bound <= strike_price <= upper_bound:
                options_data.append({
                    "Updated Date": option.get('updated_at'),
                    "Expiration Date": option.get('expiration'),
                    "Ticker": option.get('symbol'),
                    "Strike": option.get('strike'),
                    "Last": option.get('last_price'),
                    "Theor.": option.get('theoretical_price'),
                    "IV": option.get('implied_volatility'),
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
    else:
        print(f"Error fetching data for {ticker} with expiration {expiration_date}: {response.status_code} - {response.text}")
        return pd.DataFrame()

# List of tickers and expiration dates
tickers = ['spy', 'qqq', 'iwm', 'gld']
expiration_dates = ['2024-06-14', '2024-06-21', '2024-06-28']

# Your Twelve Data API key
api_key = '9c96f309b68741469886f5706aa263ac'

# Initialize an empty DataFrame to store all the data
all_data = pd.DataFrame()

# Loop through each ticker and expiration date
for ticker in tickers:
    for expiration_date in expiration_dates:
        df = get_options_chain(ticker, expiration_date, api_key)
        if not df.empty:
            all_data = pd.concat([all_data, df], ignore_index=True)
            print(all_data.head())
        else:
            print(f"No data for {ticker} with expiration {expiration_date}")

# Save the combined data to a CSV file if data is present
if not all_data.empty:
    all_data.to_csv('options_chain_data.csv', index=False)
    print("Data saved to options_chain_data.csv")
else:
    print("No data to save.")
