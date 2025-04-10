# Institutional Flow Analysis Implementation Plan

## Project Overview

This plan details the implementation of three key tools for analyzing institutional options flow:
1. **Daily Institutional Flow Scanner** - Detect unusual dark pool activity
2. **Strike Concentration Analyzer** - Identify institutional positioning across option strikes
3. **Put/Call Premium Ratio Monitor** - Track sentiment shifts through premium ratios

## System Architecture

### Directory Structure
```
institutional_flow_analysis/
├── config/
│   ├── tickers.json         # Watchlist configuration
│   ├── thresholds.json      # Alert thresholds configuration
│   └── api_keys.json        # API credentials (gitignored)
├── data/
│   ├── raw/                 # Raw API responses
│   └── processed/           # Cleaned and processed data
├── scripts/
│   ├── fetch_data.py        # API data collection script
│   ├── process_data.py      # Data cleaning and preprocessing
│   └── alert_system.py      # Alert generation logic
├── notebooks/
│   ├── flow_scanner.ipynb   # Institutional flow analysis
│   ├── strike_analyzer.ipynb # Strike concentration visualization
│   └── premium_monitor.ipynb # Put/Call ratio analysis
├── utils/
│   ├── polygon_api.py       # Polygon API wrapper
│   ├── visualization.py     # Visualization utilities
│   └── alerts.py            # Alert generation utilities
└── requirements.txt         # Project dependencies
```

## Implementation Timeline

### Phase 1: Data Collection & Storage (Week 1)
- Set up API authentication with Polygon
- Implement data fetching for options and dark pool data
- Create data storage and organization system
- Build automated daily data retrieval

### Phase 2: Data Processing & Analysis (Week 2)
- Develop data cleaning and normalization pipeline
- Implement basic analytical functions
- Create preliminary visualizations
- Set up Jupyter notebook environments

### Phase 3: Tool Development (Weeks 3-4)
- Build each analysis tool
- Implement visualization components
- Create alert system
- Connect components into workflow

## Detailed Implementation

### 1. Data Collection System

#### `fetch_data.py`
```python
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient

# Load configuration
with open('config/api_keys.json', 'r') as f:
    api_keys = json.load(f)
    
with open('config/tickers.json', 'r') as f:
    tickers = json.load(f)

# Initialize API client
client = RESTClient(api_keys['polygon'])

def fetch_options_data(date=None):
    """Fetch options data for watchlist tickers"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    data_dir = f"data/raw/options/{date}"
    os.makedirs(data_dir, exist_ok=True)
    
    for ticker in tickers['watchlist']:
        print(f"Fetching options data for {ticker}...")
        
        # Get all options contracts
        options = client.list_options_contracts(
            underlying_ticker=ticker,
            expiration_date_gte=date,
            limit=1000
        )
        
        # Process and save options data
        options_data = []
        for contract in options:
            options_data.append({
                'ticker': contract.ticker,
                'underlying': contract.underlying_ticker,
                'expiration': contract.expiration_date,
                'strike': contract.strike_price,
                'type': contract.contract_type,
                'open_interest': contract.open_interest,
                'implied_volatility': contract.implied_volatility,
            })
        
        # Save to CSV
        df = pd.DataFrame(options_data)
        df.to_csv(f"{data_dir}/{ticker}_options.csv", index=False)
        
        # Fetch options trades
        for contract in options_data[:100]:  # Limit to recent contracts to avoid API limits
            contract_ticker = contract['ticker']
            trades = client.list_trades(
                ticker=contract_ticker,
                timestamp_gte=f"{date}T00:00:00Z",
                limit=50000
            )
            
            trades_data = []
            for trade in trades:
                trades_data.append({
                    'ticker': contract_ticker,
                    'price': trade.price,
                    'size': trade.size,
                    'timestamp': trade.timestamp,
                    'exchange': trade.exchange,
                    'conditions': trade.conditions,
                })
            
            if trades_data:
                df_trades = pd.DataFrame(trades_data)
                os.makedirs(f"{data_dir}/trades", exist_ok=True)
                df_trades.to_csv(f"{data_dir}/trades/{contract_ticker}_trades.csv", index=False)

def fetch_dark_pool_data(date=None):
    """Fetch dark pool data for watchlist tickers"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    data_dir = f"data/raw/dark_pool/{date}"
    os.makedirs(data_dir, exist_ok=True)
    
    for ticker in tickers['watchlist']:
        print(f"Fetching dark pool data for {ticker}...")
        
        # Get aggregate data
        aggs = client.get_aggs(
            ticker=ticker,
            multiplier=1,
            timespan="minute",
            from_=date,
            to=date,
            limit=10000
        )
        
        # Get trades with TRF conditions (dark pool indicator)
        trades = client.list_trades(
            ticker=ticker,
            timestamp_gte=f"{date}T00:00:00Z",
            limit=50000
        )
        
        # Process dark pool trades
        dark_pool_trades = []
        for trade in trades:
            # Check for dark pool conditions
            is_dark_pool = any(cond in [
                '4', '15', '16', '19', '21', '22', 
                '23', '24', '25', '26', '27'
            ] for cond in trade.conditions)
            
            if is_dark_pool:
                dark_pool_trades.append({
                    'ticker': ticker,
                    'price': trade.price,
                    'size': trade.size,
                    'timestamp': trade.timestamp,
                    'conditions': trade.conditions,
                })
        
        if dark_pool_trades:
            df_dark_pool = pd.DataFrame(dark_pool_trades)
            df_dark_pool.to_csv(f"{data_dir}/{ticker}_dark_pool.csv", index=False)

if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    fetch_options_data(today)
    fetch_dark_pool_data(today)
```

#### `process_data.py`
```python
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob

def process_options_data(date=None):
    """Process and combine options data for analysis"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    raw_dir = f"data/raw/options/{date}"
    proc_dir = f"data/processed/options/{date}"
    os.makedirs(proc_dir, exist_ok=True)
    
    # Combine all options data
    all_files = glob.glob(f"{raw_dir}/*_options.csv")
    options_dfs = []
    
    for file in all_files:
        df = pd.read_csv(file)
        options_dfs.append(df)
    
    if options_dfs:
        all_options = pd.concat(options_dfs, ignore_index=True)
        all_options.to_csv(f"{proc_dir}/all_options.csv", index=False)
    
    # Process trades to find institutional flow
    trade_files = glob.glob(f"{raw_dir}/trades/*.csv")
    
    # Combine all trades
    trades_dfs = []
    for file in trade_files:
        df = pd.read_csv(file)
        trades_dfs.append(df)
    
    if trades_dfs:
        all_trades = pd.concat(trades_dfs, ignore_index=True)
        
        # Calculate trade value (premium)
        all_trades['premium'] = all_trades['price'] * all_trades['size'] * 100  # x100 for option contracts
        
        # Filter for large trades (institutional)
        large_trades = all_trades[all_trades['premium'] > 100000]  # Trades over $100k
        
        # Save large trades
        large_trades.to_csv(f"{proc_dir}/institutional_flow.csv", index=False)
        
        # Create aggregated data
        option_info = all_options[['ticker', 'underlying', 'strike', 'type', 'expiration']]
        large_trades_with_info = pd.merge(
            large_trades, 
            option_info, 
            on='ticker', 
            how='left'
        )
        
        # Calculate sentiment based on option type and trade direction
        large_trades_with_info['sentiment'] = large_trades_with_info.apply(
            lambda row: 'bullish' if (row['type'] == 'call') else 'bearish',
            axis=1
        )
        
        large_trades_with_info.to_csv(f"{proc_dir}/institutional_flow_with_info.csv", index=False)
        
        return large_trades_with_info
    
    return None

def process_dark_pool_data(date=None):
    """Process dark pool data for analysis"""
    if date is None:
        date = datetime.now().strftime('%Y-%m-%d')
    
    raw_dir = f"data/raw/dark_pool/{date}"
    proc_dir = f"data/processed/dark_pool/{date}"
    os.makedirs(proc_dir, exist_ok=True)
    
    # Combine all dark pool data
    all_files = glob.glob(f"{raw_dir}/*_dark_pool.csv")
    dp_dfs = []
    
    for file in all_files:
        ticker = os.path.basename(file).replace('_dark_pool.csv', '')
        df = pd.read_csv(file)
        df['ticker'] = ticker
        dp_dfs.append(df)
    
    if dp_dfs:
        all_dark_pool = pd.concat(dp_dfs, ignore_index=True)
        
        # Calculate trade value
        all_dark_pool['value'] = all_dark_pool['price'] * all_dark_pool['size']
        
        # Aggregate by ticker
        ticker_summary = all_dark_pool.groupby('ticker').agg({
            'size': 'sum',
            'value': 'sum',
            'price': 'mean',
            'timestamp': 'count'
        }).reset_index()
        ticker_summary.rename(columns={'timestamp': 'trade_count'}, inplace=True)
        
        # Save processed data
        all_dark_pool.to_csv(f"{proc_dir}/all_dark_pool.csv", index=False)
        ticker_summary.to_csv(f"{proc_dir}/dark_pool_summary.csv", index=False)
        
        return all_dark_pool, ticker_summary
    
    return None, None

if __name__ == "__main__":
    today = datetime.now().strftime('%Y-%m-%d')
    process_options_data(today)
    process_dark_pool_data(today)
```

### 2. Daily Institutional Flow Scanner

#### `flow_scanner.ipynb`
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import json

# Load configuration
with open('../config/thresholds.json', 'r') as f:
    thresholds = json.load(f)

with open('../config/tickers.json', 'r') as f:
    tickers = json.load(f)

# Set the date (today by default)
date = datetime.now().strftime('%Y-%m-%d')

# Load processed data
flow_data_path = f"../data/processed/options/{date}/institutional_flow_with_info.csv"
dark_pool_path = f"../data/processed/dark_pool/{date}/dark_pool_summary.csv"

flow_data = pd.read_csv(flow_data_path)
dark_pool_data = pd.read_csv(dark_pool_path)

# 1. Find unusual option flows
# Group by underlying ticker, type, and expiration
flow_by_ticker = flow_data.groupby(['underlying', 'type', 'expiration']).agg({
    'premium': 'sum',
    'ticker': 'count'
}).reset_index()
flow_by_ticker.rename(columns={'ticker': 'trade_count'}, inplace=True)

# Calculate historical averages (would require historical data)
# For now, we'll use thresholds

# Identify unusual flows
unusual_flow = flow_by_ticker[flow_by_ticker['premium'] > thresholds['unusual_premium']]
unusual_flow = unusual_flow.sort_values('premium', ascending=False)

# 2. Analyze dark pool activity
# Calculate average trade size
dark_pool_data['avg_trade_size'] = dark_pool_data['size'] / dark_pool_data['trade_count']
dark_pool_data['avg_trade_value'] = dark_pool_data['value'] / dark_pool_data['trade_count']

# Identify unusual dark pool activity
unusual_dark_pool = dark_pool_data[
    dark_pool_data['avg_trade_value'] > thresholds['unusual_dark_pool_value']
]
unusual_dark_pool = unusual_dark_pool.sort_values('value', ascending=False)

# 3. Display results
print("=== UNUSUAL OPTIONS FLOW ===")
display(unusual_flow.head(10))

print("\n=== UNUSUAL DARK POOL ACTIVITY ===")
display(unusual_dark_pool.head(10))

# 4. Visualize the data
plt.figure(figsize=(12, 6))
sns.barplot(data=unusual_flow.head(10), x='underlying', y='premium', hue='type')
plt.title('Top 10 Unusual Options Flow')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# 5. Identify potential trade setups
# Combine options flow with dark pool data
potential_setups = []

for ticker in unusual_flow['underlying'].unique():
    # Check if there's dark pool activity for this ticker
    if ticker in unusual_dark_pool['ticker'].values:
        # Get options flow
        ticker_flow = unusual_flow[unusual_flow['underlying'] == ticker]
        
        # Get dark pool data
        ticker_dp = unusual_dark_pool[unusual_dark_pool['ticker'] == ticker]
        
        # Determine sentiment
        bullish_premium = ticker_flow[ticker_flow['type'] == 'call']['premium'].sum()
        bearish_premium = ticker_flow[ticker_flow['type'] == 'put']['premium'].sum()
        
        sentiment = 'bullish' if bullish_premium > bearish_premium else 'bearish'
        
        # Add to potential setups
        potential_setups.append({
            'ticker': ticker,
            'sentiment': sentiment,
            'options_premium': bullish_premium + bearish_premium,
            'dark_pool_value': ticker_dp['value'].values[0],
            'confidence': 'high' if abs(bullish_premium - bearish_premium) > thresholds['sentiment_difference'] else 'medium'
        })

potential_setups_df = pd.DataFrame(potential_setups)
print("\n=== POTENTIAL TRADE SETUPS ===")
display(potential_setups_df.sort_values('options_premium', ascending=False))

# 6. Save alerts to file
if not potential_setups_df.empty:
    alerts_dir = f"../data/alerts/{date}"
    os.makedirs(alerts_dir, exist_ok=True)
    potential_setups_df.to_csv(f"{alerts_dir}/potential_setups.csv", index=False)
    print(f"Alerts saved to {alerts_dir}/potential_setups.csv")
```

### 3. Strike Concentration Analyzer

#### `strike_analyzer.ipynb`
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import os
import json

# Load configuration
with open('../config/thresholds.json', 'r') as f:
    thresholds = json.load(f)

with open('../config/tickers.json', 'r') as f:
    tickers = json.load(f)

# Set the date (today by default)
date = datetime.now().strftime('%Y-%m-%d')

# Load processed data
flow_data_path = f"../data/processed/options/{date}/institutional_flow_with_info.csv"
flow_data = pd.read_csv(flow_data_path)

# Get available tickers
available_tickers = flow_data['underlying'].unique()

# Function to analyze strike concentration for a ticker
def analyze_strike_concentration(ticker, expiration=None):
    # Filter data
    ticker_data = flow_data[flow_data['underlying'] == ticker]
    
    if expiration:
        ticker_data = ticker_data[ticker_data['expiration'] == expiration]
    
    # Get available expirations
    expirations = ticker_data['expiration'].unique()
    
    if not expiration:
        # If no expiration provided, use the one with the most activity
        expiration_counts = ticker_data.groupby('expiration')['premium'].sum()
        expiration = expiration_counts.idxmax()
        ticker_data = ticker_data[ticker_data['expiration'] == expiration]
    
    # Group by strike and type
    strike_data = ticker_data.groupby(['strike', 'type']).agg({
        'premium': 'sum',
        'ticker': 'count'
    }).reset_index()
    strike_data.rename(columns={'ticker': 'trade_count'}, inplace=True)
    
    # Pivot to get separate columns for call and put
    strike_pivot = strike_data.pivot(index='strike', columns='type', values='premium').reset_index()
    strike_pivot.fillna(0, inplace=True)
    
    # Calculate total premium and ratio
    strike_pivot['total'] = strike_pivot['call'] + strike_pivot['put']
    strike_pivot['call_ratio'] = strike_pivot['call'] / strike_pivot['total']
    strike_pivot['put_ratio'] = strike_pivot['put'] / strike_pivot['total']
    
    # Sort by total premium
    strike_pivot = strike_pivot.sort_values('total', ascending=False)
    
    # Identify concentration points
    concentration_threshold = thresholds['strike_concentration_pct'] * strike_pivot['total'].sum()
    concentration_points = strike_pivot[strike_pivot['total'] > concentration_threshold]
    
    # Visualize
    plt.figure(figsize=(12, 8))
    
    # Plot premium by strike
    plt.subplot(2, 1, 1)
    plt.bar(strike_pivot['strike'], strike_pivot['call'], label='Call Premium', alpha=0.7, color='green')
    plt.bar(strike_pivot['strike'], strike_pivot['put'], bottom=strike_pivot['call'], 
           label='Put Premium', alpha=0.7, color='red')
    
    for strike in concentration_points['strike']:
        plt.axvline(x=strike, color='black', linestyle='--', alpha=0.3)
    
    plt.title(f'Options Premium Distribution by Strike - {ticker} (Exp: {expiration})')
    plt.xlabel('Strike Price')
    plt.ylabel('Premium ($)')
    plt.legend()
    plt.grid(alpha=0.3)
    
    # Plot put/call ratio
    plt.subplot(2, 1, 2)
    plt.bar(strike_pivot['strike'], strike_pivot['put_ratio'], label='Put Ratio', alpha=0.7, color='red')
    plt.bar(strike_pivot['strike'], strike_pivot['call_ratio'], label='Call Ratio', alpha=0.7, color='green')
    
    for strike in concentration_points['strike']:
        plt.axvline(x=strike, color='black', linestyle='--', alpha=0.3)
    
    plt.title(f'Put/Call Ratio by Strike - {ticker} (Exp: {expiration})')
    plt.xlabel('Strike Price')
    plt.ylabel('Ratio')
    plt.legend()
    plt.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.show()
    
    return concentration_points, strike_pivot, expirations

# For the notebook, analyze SPY as an example
ticker = 'SPY'  # This can be a dropdown in the notebook
concentration_points, strike_data, available_expirations = analyze_strike_concentration(ticker)

# Display concentration points
print(f"=== STRIKE CONCENTRATION POINTS FOR {ticker} ===")
display(concentration_points)

# Save results
results_dir = f"../data/results/strike_concentration/{date}"
os.makedirs(results_dir, exist_ok=True)
concentration_points.to_csv(f"{results_dir}/{ticker}_concentration.csv", index=False)
strike_data.to_csv(f"{results_dir}/{ticker}_strike_data.csv", index=False)

# Function to identify potential support/resistance levels
def identify_support_resistance(ticker, concentration_points):
    # This would use additional price data to compare with option concentration
    # For now, we'll use a simple approach based on premium concentration
    
    support_levels = []
    resistance_levels = []
    
    for _, row in concentration_points.iterrows():
        strike = row['strike']
        put_premium = row['put']
        call_premium = row['call']
        
        if put_premium > call_premium:
            # More put premium could indicate support level
            support_levels.append({
                'strike': strike,
                'premium': put_premium,
                'strength': put_premium / (put_premium + call_premium)
            })
        else:
            # More call premium could indicate resistance level
            resistance_levels.append({
                'strike': strike,
                'premium': call_premium,
                'strength': call_premium / (put_premium + call_premium)
            })
    
    return pd.DataFrame(support_levels), pd.DataFrame(resistance_levels)

support_df, resistance_df = identify_support_resistance(ticker, concentration_points)

print("\n=== POTENTIAL SUPPORT LEVELS ===")
display(support_df.sort_values('premium', ascending=False))

print("\n=== POTENTIAL RESISTANCE LEVELS ===")
display(resistance_df.sort_values('premium', ascending=False))

# Save support/resistance levels
support_df.to_csv(f"{results_dir}/{ticker}_support_levels.csv", index=False)
resistance_df.to_csv(f"{results_dir}/{ticker}_resistance_levels.csv", index=False)
```

### 4. Put/Call Premium Ratio Monitor

#### `premium_monitor.ipynb`
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, date
import os
import json
import glob

# Load configuration
with open('../config/thresholds.json', 'r') as f:
    thresholds = json.load(f)

with open('../config/tickers.json', 'r') as f:
    tickers = json.load(f)

# Set the date (today by default)
today = datetime.now().strftime('%Y-%m-%d')

# Function to load multiple days of data
def load_historical_flow(days=5):
    combined_data = []
    
    for i in range(days):
        date_to_load = (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d')
        file_path = f"../data/processed/options/{date_to_load}/institutional_flow_with_info.csv"
        
        if os.path.exists(file_path):
            data = pd.read_csv(file_path)
            data['date'] = date_to_load
            combined_data.append(data)
    
    if combined_data:
        return pd.concat(combined_data, ignore_index=True)
    return None

# Load historical data
historical_flow = load_historical_flow(7)  # One week of data

if historical_flow is not None:
    # Calculate daily put/call ratio by ticker
    daily_sentiment = historical_flow.groupby(['underlying', 'date', 'type']).agg({
        'premium': 'sum'
    }).reset_index()
    
    # Pivot to get call and put columns
    daily_pivot = daily_sentiment.pivot_table(
        index=['underlying', 'date'], 
        columns='type', 
        values='premium'
    ).reset_index()
    
    # Fill NaN values with 0
    daily_pivot = daily_pivot.fillna(0)
    
    # Calculate put/call ratio
    daily_pivot['put_call_ratio'] = daily_pivot['put'] / daily_pivot['call']
    daily_pivot['call_put_ratio'] = daily_pivot['call'] / daily_pivot['put']
    
    # Replace infinite values with a large number
    daily_pivot = daily_pivot.replace([np.inf, -np.inf], 100)
    
    # Calculate total premium
    daily_pivot['total_premium'] = daily_pivot['call'] + daily_pivot['put']
    
    # Sort by date
    daily_pivot = daily_pivot.sort_values(['underlying', 'date'])
    
    # Function to analyze put/call ratio for a ticker
    def analyze_put_call_ratio(ticker):
        # Filter data for the ticker
        ticker_data = daily_pivot[daily_pivot['underlying'] == ticker]
        
        if ticker_data.empty:
            print(f"No data available for {ticker}")
            return None
        
        # Calculate ratio changes
        ticker_data['ratio_change'] = ticker_data['put_call_ratio'].diff()
        
        # Check for significant changes
        significant_change = abs(ticker_data['ratio_change']) > thresholds['significant_ratio_change']
        ticker_data['significant_change'] = significant_change
        
        # Visualize
        plt.figure(figsize=(12, 8))
        
        # Plot put/call ratio
        plt.subplot(2, 1, 1)
        plt.plot(ticker_data['date'], ticker_data['put_call_ratio'], 'o-', label='Put/Call Ratio')
        plt.axhline(y=1, color='gray', linestyle='--', alpha=0.7)
        
        for i, row in ticker_data.iterrows():
            if row['significant_change']:
                plt.scatter(row['date'], row['put_call_ratio'], color='red', s=100, zorder=5)
        
        plt.title(f'Put/Call Ratio - {ticker}')
        plt.xlabel('Date')
        plt.ylabel('Ratio')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.xticks(rotation=45)
        
        # Plot premium
        plt.subplot(2, 1, 2)
        plt.bar(ticker_data['date'], ticker_data['call'], label='Call Premium', alpha=0.7, color='green')
        plt.bar(ticker_data['date'], ticker_data['put'], bottom=ticker_data['call'], 
               label='Put Premium', alpha=0.7, color='red')
        
        plt.title(f'Premium Distribution - {ticker}')
        plt.xlabel('Date')
        plt.ylabel('Premium ($)')
        plt.legend()
        plt.grid(alpha=0.3)
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        plt.show()
        
        return ticker_data
    
    # Analyze the tickers in the watchlist
    results = {}
    
    for ticker in tickers['watchlist']:
        if ticker in daily_pivot['underlying'].unique():
            print(f"\n=== PUT/CALL RATIO ANALYSIS FOR {ticker} ===")
            result = analyze_put_call_ratio(ticker)
            if result is not None:
                results[ticker] = result
    
    # Identify sentiment shifts
    sentiment_shifts = []
    
    for ticker, data in results.items():
        # Get the most recent data point
        latest = data.iloc[-1]
        
        if latest['significant_change']:
            shift_direction = 'bearish' if latest['ratio_change'] > 0 else 'bullish'
            
            sentiment_shifts.append({
                'ticker': ticker,
                'date': latest['date'],
                'put_call_ratio': latest['put_call_ratio'],
                'ratio_change': latest['ratio_change'],
                'shift': shift_direction,
                'total_premium': latest['total_premium']
            })
    
    sentiment_shifts_df = pd.DataFrame(sentiment_shifts)
    
    if not sentiment_shifts_df.empty:
        print("\n=== SIGNIFICANT SENTIMENT SHIFTS ===")
        display(sentiment_shifts_df.sort_values('total_premium', ascending=False))
        
        # Save alerts
        alerts_dir = f"../data/alerts/{today}"
        os.makedirs(alerts_dir, exist_ok=True)
        sentiment_shifts_df.to_csv(f"{alerts_dir}/sentiment_shifts.csv", index=False)
        print(f"Sentiment shift alerts saved to {alerts_dir}/sentiment_shifts.csv")
    else:
        print("\nNo significant sentiment shifts detected.")
    
    # Calculate market-wide sentiment
    market_sentiment = daily_pivot.groupby('date').agg({
        'call': 'sum',
        'put': 'sum'
    }).reset_index()
    
    market_sentiment['put_call_ratio'] = market_sentiment['put'] / market_sentiment['call']
    market_sentiment['ratio_change'] = market_sentiment['put_call_ratio'].diff()
    
    print("\n=== MARKET-WIDE SENTIMENT ===")
    display(market_sentiment)
    
    # Plot market-wide sentiment
    plt.figure(figsize=(10, 6))
    plt.plot(market_sentiment['date'], market_sentiment['put_call_ratio'], 'o-', label='Market-wide Put/Call Ratio')
    plt.axhline(y=1, color='gray', linestyle='--', alpha=0.7)
    plt.title('Market-wide Put/Call Ratio')
    plt.xlabel('Date')
    plt.ylabel('Ratio')
    plt.legend()
    plt.grid(alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    
    # Save market sentiment
    results_dir = f"../data/results/sentiment/{today}"
    os.makedirs(results_dir, exist_ok=True)
    market_sentiment.to_csv(f"{results_dir}/market_sentiment.csv", index=False)
else:
    print("No historical data available. Please run data collection scripts first.")
```

### 5. Alert System

#### `alert_system.py`
```python
import pandas as pd
import json
import os
from datetime import datetime
import smtplib
from