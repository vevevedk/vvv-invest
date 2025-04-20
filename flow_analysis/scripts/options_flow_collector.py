#!/usr/bin/env python3

import os
import time
import logging
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional
import pandas as pd
import requests
from dotenv import load_dotenv

from flow_analysis.config.watchlist import SYMBOLS
from flow_analysis.db.connection import get_db_connection

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"

class OptionsFlowCollector:
    def __init__(self):
        self.api_key = os.getenv("UW_API_TOKEN")
        if not self.api_key:
            raise ValueError("UW_API_TOKEN environment variable not set")
        
        self.base_url = "https://api.unusualwhales.com/api"
        self.eastern = pytz.timezone('US/Eastern')
        self.db_conn = get_db_connection()
    
    def _process_flow(self, flow_data: List[Dict]) -> pd.DataFrame:
        """Process raw flow data into a DataFrame"""
        if not flow_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(flow_data)
        
        # Filter for our watchlist symbols
        df = df[df['symbol'].isin(SYMBOLS)]
        
        # Convert timestamp to datetime
        df['collected_at'] = pd.to_datetime(df['timestamp']).dt.tz_localize('UTC')
        
        # Select and rename columns
        df = df[[
            'symbol',
            'strike',
            'expiry',
            'flow_type',
            'premium',
            'contract_size',
            'iv_rank',
            'collected_at'
        ]]
        
        return df
    
    def _save_flow_to_db(self, df: pd.DataFrame) -> None:
        """Save processed flow data to database"""
        if df.empty:
            return
        
        with self.db_conn.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute("""
                    INSERT INTO trading.options_flow (
                        symbol, strike, expiry, flow_type, premium,
                        contract_size, iv_rank, collected_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['symbol'],
                    row['strike'],
                    row['expiry'],
                    row['flow_type'],
                    row['premium'],
                    row['contract_size'],
                    row['iv_rank'],
                    row['collected_at']
                ))
            
            self.db_conn.commit()
            logger.info(f"Inserted {len(df)} options flow records")
    
    def collect_flow(self) -> pd.DataFrame:
        """Collect options flow data from API"""
        try:
            response = requests.get(
                f"{self.base_url}/options/flow",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=10
            )
            response.raise_for_status()
            
            flow_data = response.json()
            df = self._process_flow(flow_data)
            
            if not df.empty:
                self._save_flow_to_db(df)
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            return pd.DataFrame()
    
    def is_market_open(self) -> bool:
        """Check if the market is currently open"""
        current_time = datetime.now(self.eastern)
        
        # Check if it's a weekday
        if current_time.weekday() >= 5:
            logger.info(f"Market closed - weekend ({current_time.strftime('%A')})")
            return False
        
        # Check if it's within market hours
        market_open = current_time.replace(
            hour=int(MARKET_OPEN.split(':')[0]),
            minute=int(MARKET_OPEN.split(':')[1]),
            second=0,
            microsecond=0
        )
        market_close = current_time.replace(
            hour=int(MARKET_CLOSE.split(':')[0]),
            minute=int(MARKET_CLOSE.split(':')[1]),
            second=0,
            microsecond=0
        )
        
        is_open = market_open <= current_time <= market_close
        if not is_open:
            logger.info(f"Market closed - outside trading hours (Current: {current_time.strftime('%H:%M')} ET)")
        return is_open
    
    def get_next_market_open(self) -> datetime:
        """Get the next market open time"""
        current_time = datetime.now(self.eastern)
        
        # Start with current day's market open
        next_open = current_time.replace(
            hour=int(MARKET_OPEN.split(':')[0]),
            minute=int(MARKET_OPEN.split(':')[1]),
            second=0,
            microsecond=0
        )
        
        # If we're past today's market open, move to next business day
        if current_time >= next_open:
            next_open += timedelta(days=1)
        
        # Skip weekends
        while next_open.weekday() >= 5:
            next_open += timedelta(days=1)
        
        return next_open
    
    def run(self, collection_interval: int = 300):  # 5 minutes in seconds
        """Run the collector continuously during market hours"""
        logger.info("Starting options flow collector...")
        
        while True:
            if not self.is_market_open():
                next_open = self.get_next_market_open()
                wait_seconds = (next_open - datetime.now(self.eastern)).total_seconds()
                logger.info(f"Market closed. Sleeping until next market open: {next_open.strftime('%Y-%m-%d %H:%M')} ET ({wait_seconds/3600:.1f} hours)")
                time.sleep(wait_seconds)
                continue
            
            logger.info("Collecting options flow...")
            flow = self.collect_flow()
            if flow.empty:
                logger.info("No new options flow collected")
            
            time.sleep(collection_interval)

def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Collect options flow data')
    parser.add_argument('--interval', type=int, default=300,
                      help='Collection interval in seconds (default: 300)')
    args = parser.parse_args()
    
    # Run collector
    collector = OptionsFlowCollector()
    collector.run(collection_interval=args.interval)

if __name__ == '__main__':
    main() 