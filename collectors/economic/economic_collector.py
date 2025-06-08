#!/usr/bin/env python3

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.db_config import get_db_config
from flow_analysis.config.api_config import UW_API_TOKEN

class EconomicCollector:
    """Collector for economic events and calendar data."""
    
    BASE_URL = "https://api.unusualwhales.com/api"
    ENDPOINT = "/market/economic-calendar"
    
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        """Initialize the economic collector.
        
        Args:
            db_config: Database configuration dictionary. If None, will be loaded from config.
        """
        self.logger = self._setup_logger()
        self.api_key = UW_API_TOKEN
        if not self.api_key:
            raise ValueError("UW_API_TOKEN environment variable is not set")
        
        self.db_config = db_config or get_db_config()
        self.engine = create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )
        
        # Ensure the table exists
        self._create_table_if_not_exists()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up and return a logger instance."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _create_table_if_not_exists(self):
        """Create the economic_events table if it doesn't exist."""
        create_table_query = text("""
            CREATE TABLE IF NOT EXISTS trading.economic_events (
                id SERIAL PRIMARY KEY,
                event VARCHAR(255) NOT NULL,
                forecast VARCHAR(50),
                prev VARCHAR(50),
                reported_period VARCHAR(50),
                event_time TIMESTAMP WITH TIME ZONE NOT NULL,
                type VARCHAR(50) NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(create_table_query)
                conn.commit()
            self.logger.info("Economic events table created or already exists")
        except Exception as e:
            self.logger.error(f"Error creating economic events table: {str(e)}")
            raise
    
    def get_economic_calendar(self) -> Optional[list]:
        """Get economic calendar data from the API.
        
        Returns:
            List of economic events or None if the request fails.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        
        try:
            response = requests.get(
                f"{self.BASE_URL}{self.ENDPOINT}",
                headers=headers
            )
            response.raise_for_status()
            return response.json().get("data", [])
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching economic calendar: {str(e)}")
            return None
    
    def _process_event_data(self, events: list) -> pd.DataFrame:
        """Process raw event data into a DataFrame.
        
        Args:
            events: List of economic events from the API.
            
        Returns:
            DataFrame containing processed event data.
        """
        if not events:
            return pd.DataFrame()
        
        processed_data = []
        for event in events:
            processed_data.append({
                "event": event["event"],
                "forecast": event.get("forecast"),
                "prev": event.get("prev"),
                "reported_period": event.get("reported_period"),
                "event_time": pd.to_datetime(event["time"]).tz_convert("UTC"),
                "type": event["type"]
            })
        
        return pd.DataFrame(processed_data)
    
    def save_to_database(self, df: pd.DataFrame) -> int:
        """Save economic events to the database.
        
        Args:
            df: DataFrame containing economic events.
            
        Returns:
            Number of events saved.
        """
        if df.empty:
            return 0
        
        try:
            with self.engine.connect() as conn:
                df.to_sql(
                    "economic_events",
                    conn,
                    schema="trading",
                    if_exists="append",
                    index=False
                )
                conn.commit()
            
            self.logger.info(f"Saved {len(df)} economic events to database")
            return len(df)
        except Exception as e:
            self.logger.error(f"Error saving economic events to database: {str(e)}")
            return 0
    
    def collect(self, start_date=None, end_date=None) -> int:
        """Collect and save economic events for the specified date range.
        
        Args:
            start_date: Start date for collection. If None, defaults to 7 days ago.
            end_date: End date for collection. If None, defaults to the current date.
            
        Returns:
            Number of events collected and saved.
        """
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Starting economic events collection from {start_date} to {end_date}")
        
        try:
            # Get economic calendar data
            events = self.get_economic_calendar()
            if not events:
                self.logger.warning("No economic events found")
                return 0
            
            # Process the data
            df = self._process_event_data(events)
            if df.empty:
                self.logger.warning("No economic events to save after processing")
                return 0
            
            # Save to database
            num_saved = self.save_to_database(df)
            self.logger.info(f"Economic events collection completed. Saved {num_saved} events")
            return num_saved
            
        except Exception as e:
            self.logger.error(f"Error in economic events collection: {str(e)}")
            return 0
    
    def backfill(self, start_date=None, end_date=None, days=7):
        """Backfill economic events for the specified date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=days)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Backfilling economic events from {start_date} to {end_date}")
        self.collect(start_date=start_date, end_date=end_date)

    def get_all_events(self) -> pd.DataFrame:
        """Get all economic events from the database."""
        try:
            with self.engine.connect() as conn:
                query = "SELECT * FROM trading.economic_events ORDER BY event_time DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            self.logger.error(f"Error fetching economic events from database: {str(e)}")
            raise

if __name__ == "__main__":
    collector = EconomicCollector()
    collector.collect() 