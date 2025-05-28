#!/usr/bin/env python3

import os
import sys
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.db_config import get_db_config, SCHEMA_NAME, TABLE_NAME

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('export_darkpool_trades.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def export_darkpool_trades():
    """Export dark pool trades from the local database to a CSV file."""
    try:
        # Create exports directory if it doesn't exist
        exports_dir = Path("exports")
        exports_dir.mkdir(exist_ok=True)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = exports_dir / f"darkpool_trades_{timestamp}.csv"

        # Connect to the database
        db_config = get_db_config()
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}?sslmode={db_config['sslmode']}"
        )

        # Query to fetch all dark pool trades
        query = f"SELECT * FROM trading.darkpool_trades ORDER BY executed_at DESC;"

        # Execute the query and load results into a DataFrame
        df = pd.read_sql(query, engine)

        # Export to CSV
        df.to_csv(filename, index=False)
        logger.info(f"Exported {len(df)} trades to {filename}")

    except Exception as e:
        logger.error(f"Error exporting dark pool trades: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    export_darkpool_trades() 