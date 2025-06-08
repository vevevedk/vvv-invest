#!/usr/bin/env python3

import os
import sys
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('export_db.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
from flow_analysis.config.db_config import get_db_config

def get_existing_tables(engine):
    """Get list of existing tables in the trading schema."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'trading'
    """
    with engine.connect() as conn:
        result = conn.execute(text(query))
        return [row[0] for row in result]

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
        
        # Get list of existing tables
        existing_tables = get_existing_tables(engine)
        logger.info(f"Found {len(existing_tables)} existing tables: {', '.join(existing_tables)}")
        
        # Define tables to export (only those that exist)
        tables = {
            'darkpool_trades': 'trading.darkpool_trades',
            'news_headlines': 'trading.news_headlines',
            'economic_events': 'trading.economic_events',
            'earnings': 'trading.earnings',
            'flow_alerts': 'trading.flow_alerts'
        }
        
        # Filter to only include existing tables
        tables = {k: v for k, v in tables.items() if k in existing_tables}
        
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

def main():
    """Main entry point."""
    try:
        logger.info("Starting database export...")
        export_data_to_csv()
        logger.info("Database export completed")
    except Exception as e:
        logger.error(f"Export failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 