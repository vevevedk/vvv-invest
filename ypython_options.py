import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# Define the list of tickers, including VIX
tickers = ["^SPX","SPY", "QQQ", "IWM", "GLD", "ENPH", "PLTR", "NVDA", "MSFT", 
           "META", "AMD", "SOFI", "GOOGL", "AAPL", "TSLA", "AMZN", "^VIX", "ADBE","NIO", "XPEV", "ASML", "TSM","PPSI", "SE", "PI", "HOOD", "LCID","CHPT", "EVGO"]

#tickers = ["SPY", "^VIX"]

# Function to get the next three Fridays
def get_next_fridays(num_weeks=3):
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

# Get the list of upcoming Fridays and Wednesdays
fridays = get_next_fridays()
wednesdays = get_next_wednesdays()

# Initialize an empty DataFrame to hold the data
options_data = pd.DataFrame()

# Get today's date to add to the data
updated_date = datetime.today().strftime('%Y-%m-%d')

# Loop through each ticker and fetch options data
for ticker in tickers:
    stock = yf.Ticker(ticker)
    # Fetch all available expiration dates for the ticker
    all_expirations = stock.options

    # Determine the relevant expiration dates based on the ticker
    if ticker == "^VIX":
        relevant_expirations = [exp for exp in all_expirations if exp in wednesdays]
    else:
        relevant_expirations = [exp for exp in all_expirations if exp in fridays]

    for expiration in relevant_expirations:
        try:
            opt_chain = stock.option_chain(expiration)
            if opt_chain is None:
                continue

            # Process calls and puts
            for option_type in ["calls", "puts"]:
                opt_df = getattr(opt_chain, option_type)
                if opt_df.empty:
                    continue

                opt_df['Type'] = option_type.capitalize()
                opt_df['Expiration Date'] = expiration
                opt_df['Ticker'] = ticker
                opt_df['Updated Date'] = updated_date  # Add updated date to the data

                # Rename and filter relevant columns
                opt_df = opt_df.rename(columns={
                    'strike': 'Strike', 'lastPrice': 'Last', 'impliedVolatility': 'IV',
                    'delta': 'Delta', 'gamma': 'Gamma', 'theta': 'Theta', 'vega': 'Vega',
                    'rho': 'Rho', 'volume': 'Volume', 'openInterest': 'Open Int'
                })
                options_data = pd.concat([options_data, opt_df], ignore_index=True)
        except Exception as e:
            print(f"Error fetching data for {ticker} with expiration {expiration}: {e}")
            continue

# Ensure all columns exist before selecting
required_columns = ['Delta', 'Gamma', 'Theta', 'Vega', 'Rho']
for col in required_columns:
    if col not in options_data.columns:
        options_data[col] = pd.NA  # or use 0 or another default value

# Select and reorder the columns to match the desired format
options_data = options_data[[
    'Expiration Date', 'Ticker', 'Strike', 'Last', 'IV', 'Delta', 'Gamma', 'Theta', 'Vega', 'Rho', 
    'Volume', 'Open Int', 'Type', 'Updated Date'
]]

# Save the data to a CSV file
options_data.to_csv('yfinance_options_data.csv', index=False)
print("data saved to csv")
