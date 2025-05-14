# vvv_invest
stuff for investing

# Market Data Fetcher

A Python script to fetch market data from the trading database and save it to CSV files. The script supports fetching:

- Dark pool trades
- News headlines
- Options flow data

## Requirements

- Python 3.8+
- Required packages:
  - pandas
  - psycopg2-binary

## Installation

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Basic usage (fetches last 24 hours of data for SPY, QQQ, GLD):
```bash
python fetch_market_data.py
```

Custom symbols:
```bash
python fetch_market_data.py --symbols AAPL MSFT NVDA
```

Custom time range (e.g., last 48 hours):
```bash
python fetch_market_data.py --hours 48
```

Both custom symbols and time range:
```bash
python fetch_market_data.py --symbols AAPL MSFT NVDA --hours 48
```

## Output

The script saves three CSV files in the `data` directory:
- `darkpool_trades_{hours}h_{timestamp}.csv`
- `news_headlines_{hours}h_{timestamp}.csv`
- `options_flow_{hours}h_{timestamp}.csv`

Each file contains the respective data with timestamps and additional calculated fields.

## Data Fields

### Dark Pool Trades
- Basic trade information (symbol, size, price, etc.)
- Price impact calculations
- Trade type classification
- Hourly aggregations

### News Headlines
- Headline text and metadata
- Impact score and sentiment analysis
- Hourly news count

### Options Flow
- Option trade details
- Premium and size classifications
- Hourly flow aggregations

## Notes

- The script uses a single database connection for all queries
- Timestamps are automatically converted to datetime objects
- Data is saved with a timestamp in the filename for easy tracking

## Environment Variables

- `UW_API_TOKEN`: API token for both news and dark pool collectors (required)
