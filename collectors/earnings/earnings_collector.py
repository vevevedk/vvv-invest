#!/usr/bin/env python3

import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from typing import Optional, Dict, Any, List
from celery import shared_task

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from config.db_config import get_db_config
from flow_analysis.config.api_config import UW_API_TOKEN
from flow_analysis.config.watchlist import SYMBOLS

class EarningsCollector:
    BASE_URL = "https://api.unusualwhales.com/api/earnings"
    ENDPOINTS = {
        "afterhours": "/afterhours",
        "premarket": "/premarket",
        "historical": ""
    }

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.logger = self._setup_logger()
        self.api_key = UW_API_TOKEN
        if not self.api_key:
            raise ValueError("UW_API_TOKEN environment variable is not set")
        self.db_config = db_config or get_db_config()
        self.engine = create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )
        self._create_table_if_not_exists()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _create_table_if_not_exists(self):
        """Create the earnings table if it doesn't exist."""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS trading.earnings (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date TIMESTAMP WITH TIME ZONE NOT NULL,
            eps_actual DECIMAL(10,2),
            eps_estimate DECIMAL(10,2),
            eps_surprise DECIMAL(10,2),
            revenue_actual DECIMAL(20,2),
            revenue_estimate DECIMAL(20,2),
            revenue_surprise DECIMAL(20,2),
            source_endpoint VARCHAR(50),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_query))
                conn.commit()
            self.logger.info("Earnings table created or already exists")
        except Exception as e:
            self.logger.error(f"Error creating earnings table: {str(e)}")
            raise

    def _get(self, endpoint: str, params: dict = None) -> List[dict]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }
        try:
            response = requests.get(f"{self.BASE_URL}{endpoint}", headers=headers, params=params)
            response.raise_for_status()
            data = response.json().get("data", [])
            return data
        except Exception as e:
            self.logger.error(f"Error fetching {endpoint}: {str(e)}")
            return []

    def fetch_afterhours(self) -> List[dict]:
        self.logger.info("Fetching afterhours earnings for latest date...")
        return self._get(self.ENDPOINTS["afterhours"])

    def fetch_premarket(self) -> List[dict]:
        self.logger.info("Fetching premarket earnings for latest date...")
        return self._get(self.ENDPOINTS["premarket"])

    def fetch_historical(self, tickers: List[str]) -> List[dict]:
        self.logger.info(f"Fetching historical earnings for {len(tickers)} tickers...")
        all_results = []
        for ticker in tickers:
            endpoint = f"/{ticker}"
            data = self._get(endpoint)
            for item in data:
                item['symbol'] = ticker  # Ensure symbol is present
            all_results.extend(data)
        return all_results

    def normalize(self, records: List[dict], source: str) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df['source_endpoint'] = source
        return df

    def collect(self, start_date=None, end_date=None) -> pd.DataFrame:
        self.logger.info("Starting earnings collection from all endpoints...")
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=7)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        afterhours = self.fetch_afterhours()
        premarket = self.fetch_premarket()
        historical = self.fetch_historical(SYMBOLS)

        df_afterhours = self.normalize(afterhours, 'afterhours')
        df_premarket = self.normalize(premarket, 'premarket')
        df_historical = self.normalize(historical, 'historical')

        combined = pd.concat([df_afterhours, df_premarket, df_historical], ignore_index=True)
        self.logger.info(f"Combined earnings records: {len(combined)}")
        return combined

    def export_to_csv(self, df: pd.DataFrame, export_dir: str = "exports") -> str:
        if df.empty:
            self.logger.warning("No earnings data to export.")
            return ""
        os.makedirs(export_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{export_dir}/earnings_{timestamp}.csv"
        df.to_csv(filename, index=False)
        self.logger.info(f"Exported {len(df)} earnings records to {filename}")
        return filename

    def backfill(self, start_date=None, end_date=None, days=7):
        """Backfill earnings data for the specified date range."""
        if not start_date:
            start_date = datetime.now(timezone.utc) - timedelta(days=days)
        if not end_date:
            end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Backfilling earnings data from {start_date} to {end_date}")
        self.collect(start_date=start_date, end_date=end_date)

    def get_all_earnings(self) -> pd.DataFrame:
        """Get all earnings records from the database."""
        try:
            with self.engine.connect() as conn:
                query = "SELECT * FROM earnings ORDER BY date DESC"
                return pd.read_sql(query, conn)
        except Exception as e:
            self.logger.error(f"Error fetching earnings from database: {str(e)}")
            raise

@shared_task
def run_earnings_collector():
    collector = EarningsCollector()
    df = collector.collect()
    collector.export_to_csv(df)
    return "Earnings collection completed."

# Update Celery Beat schedule to run every 5 minutes
# Note: In production, this task is intended to run on a dedicated worker and beat service for the earnings collector.
CELERYBEAT_SCHEDULE = {
    'run-earnings-collector': {
        'task': 'collectors.earnings.earnings_collector.run_earnings_collector',
        'schedule': 300.0,  # Run every 5 minutes (300 seconds)
    },
}

if __name__ == "__main__":
    collector = EarningsCollector()
    df = collector.collect()
    collector.export_to_csv(df) 