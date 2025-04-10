import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import json
import re

# Define the list of tickers, including VIX
tickers = ["^SPX","SPY", "QQQ", "IWM", "GLD", "ENPH", "PLTR", "NVDA", "MSFT", 
          "META", "AMD", "SOFI", "GOOGL", "AAPL", "TSLA", "AMZN", "^VIX", 
          "ADBE","NIO", "XPEV", "ASML", "TSM","PPSI", "SE", "PI", "HOOD", 
          "LCID","CHPT", "EVGO","MSTR","QS", "ENVX", "FREY", "RCAT", "ONDS",
          "SOUN","PSNY","TLT","RKLB","TEM"]

# Function to get the next three Fridays
def get_next_fridays(num_weeks=5):
   today = datetime.today()
   fridays = []
   while len(fridays) < num_weeks:
       if today.weekday() == 4:  # 4 corresponds to Friday
           fridays.append(today.strftime('%Y-%m-%d'))
       today += timedelta(days=1)
   return fridays

# Function to get the next three Wednesdays
def get_next_wednesdays(num_weeks=3):
   today = datetime.today()
   wednesdays = []
   while len(wednesdays) < num_weeks:
       if today.weekday() == 2:  # 2 corresponds to Wednesday
           wednesdays.append(today.strftime('%Y-%m-%d'))
       today += timedelta(days=1)
   return wednesdays

def get_crumb(session):
   try:
       # First get the main page to get cookies
       main_url = "https://finance.yahoo.com"
       session.get(main_url)
       
       # Now get the options page
       url = "https://finance.yahoo.com/quote/SPY/options?p=SPY"
       headers = {
           'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'Accept-Language': 'en-US,en;q=0.5',
           'Connection': 'keep-alive',
           'Upgrade-Insecure-Requests': '1',
           'Cache-Control': 'max-age=0'
       }
       
       response = session.get(url, headers=headers)
       print(f"Crumb fetch status code: {response.status_code}")
       
       if response.status_code == 200:
           # Try multiple patterns to find the crumb
           patterns = [
               r'"crumb":"(.+?)"',
               r'CrumbStore":{"crumb":"(.+?)"',
               r'"CrumbStore":\{"crumb":"(.+?)"\}'
           ]
           
           for pattern in patterns:
               match = re.search(pattern, response.text)
               if match:
                   crumb = match.group(1)
                   print(f"Found crumb: {crumb[:10]}...")  # Print first 10 chars of crumb
                   return crumb
                   
           print("Could not find crumb in response")
           # Print a small portion of the response for debugging
           print("Response preview:")
           print(response.text[:500])
       else:
           print(f"Failed to get page, status code: {response.status_code}")
           
   except Exception as e:
       print(f"Error getting crumb: {e}")
   return None

def get_ticker_data(ticker_symbol, max_retries=3, delay=2):
   # Create a persistent session
   session = requests.Session()
   
   headers = {
       'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
       'Accept-Language': 'en-US,en;q=0.5',
       'Connection': 'keep-alive',
       'Upgrade-Insecure-Requests': '1',
       'Cache-Control': 'max-age=0'
   }
   session.headers.update(headers)
   
   for attempt in range(max_retries):
       try:
           # Get the crumb using the same session
           crumb = get_crumb(session)
           if not crumb:
               print(f"Failed to get crumb for {ticker_symbol}")
               time.sleep(delay)
               continue
           
           # Create ticker with our session
           stock = yf.Ticker(ticker_symbol, session=session)
           
           # Try to get options data with crumb
           url = f"https://query1.finance.yahoo.com/v7/finance/options/{ticker_symbol}?crumb={crumb}"
           response = session.get(url)
           
           print(f"Response status code: {response.status_code}")
           print(f"Response content preview: {response.text[:200]}")
           
           if response.status_code == 200:
               data = response.json()
               if data and 'optionChain' in data:
                   expirations = stock.options
                   if not expirations:
                       print(f"No options data available for {ticker_symbol}")
                       return None
                   return stock
           
           raise Exception(f"Failed to get valid response. Status: {response.status_code}")
           
       except Exception as e:
           print(f"Attempt {attempt + 1} failed for {ticker_symbol}: {e}")
           if attempt < max_retries - 1:
               print(f"Waiting {delay} seconds before retry...")
               time.sleep(delay)
               delay *= 2
           continue
           
   return None

# Get the list of upcoming Fridays and Wednesdays
fridays = get_next_fridays()
wednesdays = get_next_wednesdays()

# Initialize an empty DataFrame to hold the data
options_data = pd.DataFrame()

# Get today's date to add to the data
updated_date = datetime.today().strftime('%Y-%m-%d')

# Loop through each ticker and fetch options data
for ticker in tickers:
   print(f"\nProcessing {ticker}...")
   stock = get_ticker_data(ticker)
   
   if stock is None:
       print(f"Skipping {ticker} due to data fetch failure")
       continue
   
   # Add a longer delay between tickers
   time.sleep(5)  # Increased delay
   
   try:
       # Fetch all available expiration dates for the ticker
       all_expirations = stock.options
       
       # Add delay between requests
       time.sleep(1)  # Increased delay
       
       # Determine the relevant expiration dates based on the ticker
       if ticker == "^VIX":
           relevant_expirations = [exp for exp in all_expirations if exp in wednesdays]
       else:
           relevant_expirations = [exp for exp in all_expirations if exp in fridays]
           
       for expiration in relevant_expirations:
           try:
               opt_chain = stock.option_chain(expiration)
               if opt_chain is None:
                   print(f"No option chain data for {ticker} at {expiration}")
                   continue
               
               # Process calls and puts
               for option_type in ["calls", "puts"]:
                   opt_df = getattr(opt_chain, option_type)
                   if opt_df.empty:
                       continue
                   
                   opt_df['Type'] = option_type.capitalize()
                   opt_df['Expiration Date'] = expiration
                   opt_df['Ticker'] = ticker
                   opt_df['Updated Date'] = updated_date
                   
                   # Rename and filter relevant columns
                   opt_df = opt_df.rename(columns={
                       'strike': 'Strike', 
                       'lastPrice': 'Last', 
                       'impliedVolatility': 'IV',
                       'delta': 'Delta', 
                       'gamma': 'Gamma', 
                       'theta': 'Theta', 
                       'vega': 'Vega',
                       'rho': 'Rho', 
                       'volume': 'Volume', 
                       'openInterest': 'Open Int'
                   })
                   
                   options_data = pd.concat([options_data, opt_df], ignore_index=True)
                   
               # Add a small delay between expiration dates
               time.sleep(1)  # Increased delay
               
           except Exception as e:
               print(f"Error processing {ticker} expiration {expiration}: {e}")
               continue
               
   except Exception as e:
       print(f"Error processing ticker {ticker}: {e}")
       continue

# Ensure all columns exist before selecting
required_columns = ['Delta', 'Gamma', 'Theta', 'Vega', 'Rho']
for col in required_columns:
   if col not in options_data.columns:
       options_data[col] = pd.NA  # or use 0 or another default value

# Select and reorder the columns to match the desired format
columns_to_select = [
   'Expiration Date', 'Ticker', 'Strike', 'Last', 'IV', 
   'Delta', 'Gamma', 'Theta', 'Vega', 'Rho', 
   'Volume', 'Open Int', 'Type', 'Updated Date'
]

# Only select columns that exist in the DataFrame
existing_columns = [col for col in columns_to_select if col in options_data.columns]
options_data = options_data[existing_columns]

# Save the data to a CSV file
try:
   options_data.to_csv('yfinance_options_data.csv', index=False)
   print("\nData successfully saved to yfinance_options_data.csv")
   print(f"Total rows of data collected: {len(options_data)}")
except Exception as e:
   print(f"Error saving data to CSV: {e}")

# Print summary statistics
print("\nSummary of data collected:")
print(f"Number of unique tickers: {options_data['Ticker'].nunique()}")
print(f"Number of expiration dates: {options_data['Expiration Date'].nunique()}")
print("Number of options per type:")
print(options_data['Type'].value_counts())