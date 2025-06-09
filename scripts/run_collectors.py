#!/usr/bin/env python3
# Last modified: 2024-06-03 10:35:00

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
import argparse
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

print(f"Executing file: {__file__}")
print(f"Current working directory: {os.getcwd()}")

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Parse command line arguments first
parser = argparse.ArgumentParser(description='Run data collectors')
parser.add_argument('--env', type=str, default='', help='Environment to use (empty for .env)')
parser.add_argument('--backfill', action='store_true', help='Run in backfill mode')
parser.add_argument('--days', type=int, default=7, help='Number of days to backfill')
args = parser.parse_args()

# Set environment file
env_file = '.env' if not args.env else f'.env.{args.env}'
os.environ['ENV_FILE'] = env_file

# Load environment variables
load_dotenv(env_file)
print(f"ℹ️ Using environment file: {env_file}")

# Now import the collectors after environment is set
from collectors.darkpool_collector import DarkPoolCollector
from collectors.earnings.earnings_collector import EarningsCollector
from collectors.economic.economic_collector import EconomicCollector
from collectors.news.newscollector import NewsCollector
from flow_analysis.scripts.flow_alerts_collector import FlowAlertsCollector
from collectors.utils.market_utils import is_market_open, get_next_market_open
from flow_analysis.config.api_config import UW_API_TOKEN
from flow_analysis.config.db_config import get_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('collectors.log')
    ]
)
logger = logging.getLogger(__name__)

def export_data_to_csv():
    """Export all collector data to CSV files."""
    try:
        # Create exports directory if it doesn't exist
        exports_dir = Path("exports")
        exports_dir.mkdir(exist_ok=True)
        
        # Get database connection
        db_config = get_db_config()
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
        )
        
        # Define tables to export
        tables = {
            'darkpool_trades': 'trading.darkpool_trades',
            'news_headlines': 'trading.news_headlines',
            'economic_events': 'trading.economic_events',
            'earnings': 'trading.earnings',
            'flow_alerts': 'trading.flow_alerts'
        }
        
        # Export each table
        for table_name, full_table_name in tables.items():
            try:
                # Read data from database
                query = f"SELECT * FROM {full_table_name}"
                df = pd.read_sql(query, engine)
                
                if not df.empty:
                    # Generate filename with timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = exports_dir / f"{table_name}_{timestamp}.csv"
                    
                    # Export to CSV
                    df.to_csv(filename, index=False)
                    logger.info(f"Exported {len(df)} records from {table_name} to {filename}")
                else:
                    logger.warning(f"No data found in {table_name}")
                    
            except Exception as e:
                logger.error(f"Error exporting {table_name}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Error in export_data_to_csv: {str(e)}")

def run_collectors(backfill=False, days=7):
    """Run all collectors in sequence."""
    logger.info("Starting collector run sequence")
    
    # Initialize collectors
    collectors = [
        DarkPoolCollector(),
        EarningsCollector(),
        EconomicCollector(),
        NewsCollector(),
        FlowAlertsCollector(get_db_config(), UW_API_TOKEN)
    ]
    
    # Run each collector
    for collector in collectors:
        collector_name = collector.__class__.__name__
        logger.info(f"Starting {collector_name}")
        
        start_time = time.time()
        try:
            if backfill:
                collector.backfill(days=days)
            else:
                collector.run()
        except Exception as e:
            logger.error(f"Error in {collector_name}: {str(e)}")
            continue
            
        duration = time.time() - start_time
        logger.info(f"{collector_name}: {duration:.1f}s")
    
    # Export all data to CSV files
    logger.info("Exporting all collector data to CSV files...")
    export_data_to_csv()
    logger.info("Data export completed")
        
def main():
    """Main entry point."""
    try:
        run_collectors(backfill=args.backfill, days=args.days)
    except KeyboardInterrupt:
        logger.info("Collector run interrupted by user")
    except Exception as e:
        logger.error(f"Collector run failed: {str(e)}")
        sys.exit(1)
        
if __name__ == "__main__":
    main() 