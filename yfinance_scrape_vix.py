import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta

# Function to get the next three Wednesdays
def get_next_wednesdays(num_weeks=3):
    today = datetime.today()
    wednesdays = []
    while len(wednesdays) < num_weeks:
        if today.weekday() == 2:  # 2 corresponds to Wednesday
            wednesdays.append(today)
        today += timedelta(days=1)
    return wednesdays

# Get the list of upcoming Wednesdays
wednesdays = get_next_wednesdays()

# Base URL for VIX options on Yahoo Finance
base_url = "https://finance.yahoo.com/quote/%5EVIX/options?p=%5EVIX&date="

# Function to convert a date to the Yahoo Finance expiration format (Unix timestamp in seconds)
def get_expiration_timestamp(date):
    return int(date.timestamp())

# Initialize an empty DataFrame to hold the data
all_options_data = pd.DataFrame()

# Loop through each Wednesday and scrape options data
for wednesday in wednesdays:
    expiration_timestamp = get_expiration_timestamp(wednesday)
    url = f"{base_url}{expiration_timestamp}"
    
    # Send a GET request to the Yahoo Finance options page for the VIX for this expiration date
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Find the tables containing the options data
    tables = soup.find_all('table')

    if len(tables) < 2:
        print(f"Error: Could not find options tables for expiration {wednesday.strftime('%Y-%m-%d')}.")
        continue

    # Extract calls and puts tables
    calls_table = pd.read_html(str(tables[0]))[0]
    puts_table = pd.read_html(str(tables[1]))[0]

    # Add a column for the type of option and the updated date
    calls_table['Type'] = 'Call'
    puts_table['Type'] = 'Put'
    updated_date = datetime.today().strftime('%Y-%m-%d')
    calls_table['Updated Date'] = updated_date
    puts_table['Updated Date'] = updated_date

    # Add the expiration date
    expiration_date = wednesday.strftime('%Y-%m-%d')
    calls_table['Expiration Date'] = expiration_date
    puts_table['Expiration Date'] = expiration_date

    # Combine the calls and puts into one DataFrame
    options_data = pd.concat([calls_table, puts_table], ignore_index=True)

    # Append to the overall DataFrame
    all_options_data = pd.concat([all_options_data, options_data], ignore_index=True)

# Save the combined options data to a CSV file
all_options_data.to_csv('vix_options_data.csv', index=False)

print("Options data for the next three Wednesdays has been saved to vix_options_data.csv.")
