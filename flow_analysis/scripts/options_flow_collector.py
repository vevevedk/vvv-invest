#!/usr/bin/env python3

import os
import time
import logging
from datetime import datetime, timedelta
import pytz
from typing import Dict, List, Optional, Tuple
import pandas as pd
import requests
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential
import psycopg2
from psycopg2 import sql

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
MAX_RETRIES = 3
BATCH_SIZE = 1000

class OptionsFlowCollector:
    def __init__(self):
        self.api_key = os.getenv("UW_API_TOKEN")
        if not self.api_key:
            raise ValueError("UW_API_TOKEN environment variable not set")
        
        self.base_url = "https://api.unusualwhales.com/api"
        self.eastern = pytz.timezone('US/Eastern')
        self.db_conn = get_db_connection()
        self._validate_db_connection()
    
    def _validate_db_connection(self) -> None:
        """Validate database connection and permissions"""
        try:
            with self.db_conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.execute("SELECT has_table_privilege('collector', 'trading.options_flow', 'INSERT')")
                if not cur.fetchone()[0]:
                    raise PermissionError("Collector user does not have INSERT permission on trading.options_flow")
        except psycopg2.Error as e:
            logger.error(f"Database connection validation failed: {str(e)}")
            raise
    
    def _validate_flow_data(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Validate flow data before saving to database"""
        if df.empty:
            return True, "Empty dataframe"
        
        required_columns = [
            'symbol', 'strike', 'expiry', 'flow_type',
            'premium', 'contract_size', 'iv_rank', 'collected_at'
        ]
        
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            return False, f"Missing required columns: {missing_columns}"
        
        # Check data types
        try:
            df['strike'] = pd.to_numeric(df['strike'])
            df['premium'] = pd.to_numeric(df['premium'])
            df['contract_size'] = pd.to_numeric(df['contract_size'])
            df['iv_rank'] = pd.to_numeric(df['iv_rank'])
            df['collected_at'] = pd.to_datetime(df['collected_at'])
        except Exception as e:
            return False, f"Data type conversion failed: {str(e)}"
        
        # Check for invalid values
        if df['strike'].le(0).any():
            return False, "Invalid strike price (<= 0)"
        if df['premium'].lt(0).any():
            return False, "Invalid premium (< 0)"
        if df['contract_size'].le(0).any():
            return False, "Invalid contract size (<= 0)"
        if df['iv_rank'].lt(0).any() or df['iv_rank'].gt(100).any():
            return False, "Invalid IV rank (not between 0 and 100)"
        
        return True, "Validation successful"
    
    def _process_flow(self, flow_data: List[Dict]) -> pd.DataFrame:
        """Process raw flow data into a DataFrame"""
        if not flow_data:
            return pd.DataFrame()
        
        try:
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
        except Exception as e:
            logger.error(f"Error processing flow data: {str(e)}")
            return pd.DataFrame()
    
    @retry(stop=stop_after_attempt(MAX_RETRIES),
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def _save_flow_to_db(self, df: pd.DataFrame) -> None:
        """Save processed flow data to database in batches"""
        if df.empty:
            return
        
        # Validate data before saving
        is_valid, validation_msg = self._validate_flow_data(df)
        if not is_valid:
            logger.error(f"Data validation failed: {validation_msg}")
            return
        
        try:
            with self.db_conn.cursor() as cur:
                # Prepare the insert statement
                insert_query = sql.SQL("""
                    INSERT INTO trading.options_flow (
                        symbol, strike, expiry, flow_type, premium,
                        contract_size, iv_rank, collected_at
                    ) VALUES %s
                """)
                
                # Process in batches
                for i in range(0, len(df), BATCH_SIZE):
                    batch = df.iloc[i:i+BATCH_SIZE]
                    values = [tuple(row) for row in batch.itertuples(index=False)]
                    
                    # Execute batch insert
                    psycopg2.extras.execute_values(
                        cur,
                        insert_query,
                        values,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    
                    self.db_conn.commit()
                    logger.info(f"Inserted batch of {len(batch)} records")
            
            logger.info(f"Successfully inserted {len(df)} options flow records")
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}")
            self.db_conn.rollback()
            raise
    
    @retry(stop=stop_after_attempt(MAX_RETRIES),
           wait=wait_exponential(multiplier=1, min=4, max=10))
    def collect_flow(self, historical_date: Optional[datetime] = None) -> pd.DataFrame:
        """Collect options flow data from API"""
        try:
            url = f"{self.base_url}/v2/options/flow"
            params = {}
            
            if historical_date:
                # Convert to Eastern time and set market hours
                target_date = historical_date.replace(
                    hour=int(MARKET_OPEN.split(':')[0]),
                    minute=int(MARKET_OPEN.split(':')[1])
                )
                market_close = historical_date.replace(
                    hour=int(MARKET_CLOSE.split(':')[0]),
                    minute=int(MARKET_CLOSE.split(':')[1])
                )
                
                params.update({
                    'date': historical_date.strftime('%Y-%m-%d'),
                    'start_time': target_date.strftime('%H:%M'),
                    'end_time': market_close.strftime('%H:%M')
                })
                logger.info(f"Collecting historical data for {historical_date.strftime('%Y-%m-%d')} between {target_date.strftime('%H:%M')} and {market_close.strftime('%H:%M')} ET")
            
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                params=params,
                timeout=30  # Increased timeout for historical data
            )
            response.raise_for_status()
            
            flow_data = response.json()
            if isinstance(flow_data, dict) and 'error' in flow_data:
                logger.error(f"API error: {flow_data['error']}")
                return pd.DataFrame()
                
            df = self._process_flow(flow_data)
            
            if not df.empty:
                self._save_flow_to_db(df)
                logger.info(f"Collected {len(df)} flow records")
            else:
                logger.info("No flow data found")
            
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                logger.error(f"Response text: {e.response.text}")
            raise

    def collect_historical_data(self, date: datetime) -> pd.DataFrame:
        """Collect historical data for a specific date"""
        logger.info(f"Starting historical data collection for {date.strftime('%Y-%m-%d')}")
        return self.collect_flow(historical_date=date)

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
    
    def run(self, collection_interval: int = 300, historical_date: Optional[datetime] = None):
        """Run the collector continuously during market hours or collect historical data"""
        logger.info("Starting options flow collector...")
        
        if historical_date:
            historical_date = historical_date.replace(tzinfo=self.eastern)
            self.collect_historical_data(historical_date)
            return
        
        while True:
            try:
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
                
            except Exception as e:
                logger.error(f"Error in collector run loop: {str(e)}")
                time.sleep(60)  # Wait a minute before retrying

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
    parser.add_argument('--historical', action='store_true',
                      help='Collect historical data for a specific date')
    parser.add_argument('--date', type=str,
                      help='Date for historical data collection (YYYY-MM-DD)')
    args = parser.parse_args()
    
    # Run collector
    collector = OptionsFlowCollector()
    
    if args.historical:
        if not args.date:
            historical_date = datetime(2025, 4, 17)  # Last market day
        else:
            historical_date = datetime.strptime(args.date, '%Y-%m-%d')
        collector.run(historical_date=historical_date)
    else:
        collector.run(collection_interval=args.interval)

if __name__ == '__main__':
    main() 