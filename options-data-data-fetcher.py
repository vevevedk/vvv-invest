#!/usr/bin/env python3
"""
Script to fetch options data from Polygon API
"""

import argparse
import datetime
import logging
from pathlib import Path
import sys

# Add the current directory to the Python path
sys.path.append(str(Path(__file__).parent))

from options_flow.scripts.data_fetcher import OptionsDataFetcher
from options_flow.config.api_config import POLYGON_API_KEY

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_fetcher.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Fetch options data from Polygon API')
    parser.add_argument('--date', type=str, help='Date to fetch data for (YYYY-MM-DD)')
    parser.add_argument('--data-types', type=str, nargs='+', 
                       choices=['contracts', 'trades', 'aggregates', 'dark_pool'],
                       default=['contracts', 'trades', 'aggregates', 'dark_pool'],
                       help='Types of data to fetch')
    args = parser.parse_args()

    # Parse date
    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        date = datetime.date.today()

    # Initialize fetcher
    fetcher = OptionsDataFetcher(POLYGON_API_KEY)
    
    # Fetch specified data types
    for data_type in args.data_types:
        logger.info(f"Fetching {data_type} data for {date}")
        if data_type == 'contracts':
            for ticker in ['SPY', 'QQQ']:
                contracts = fetcher.fetch_options_contracts(ticker)
                logger.info(f"Fetched {len(contracts)} contracts for {ticker}")
        elif data_type == 'trades':
            for ticker in ['SPY', 'QQQ']:
                trades = fetcher.fetch_options_trades(ticker, date)
                logger.info(f"Fetched {len(trades)} trades for {ticker}")
        elif data_type == 'aggregates':
            for ticker in ['SPY', 'QQQ']:
                aggregates = fetcher.fetch_options_aggregates(ticker, date)
                logger.info(f"Fetched {len(aggregates)} aggregates for {ticker}")
        elif data_type == 'dark_pool':
            for ticker in ['SPY', 'QQQ']:
                dark_pool = fetcher.fetch_dark_pool_estimates(ticker, date)
                logger.info(f"Fetched {len(dark_pool)} dark pool estimates for {ticker}")

if __name__ == '__main__':
    main() 