#!/usr/bin/env python3

"""
Test script for the Dark Pool Collector
Tests API connectivity, data fetching, processing, and database operations
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flow_analysis.scripts.darkpool_collector import DarkPoolCollector
from flow_analysis.config.api_config import (
    UW_BASE_URL, DARKPOOL_RECENT_ENDPOINT,
    DEFAULT_HEADERS, REQUEST_TIMEOUT
)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_env_vars():
    """Test if all required environment variables are set"""
    logger.info("Testing environment variables...")
    required_vars = [
        'UW_API_TOKEN',
        'DB_NAME',
        'DB_USER',
        'DB_PASSWORD',
        'DB_HOST',
        'DB_PORT'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("All required environment variables are set")
    return True

def test_db_connection():
    """Test database connection"""
    logger.info("Testing database connection...")
    try:
        collector = DarkPoolCollector()
        collector.connect_db()
        logger.info("Successfully connected to database")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False

def test_api_connection():
    """Test API connection"""
    logger.info("Testing API connection...")
    collector = DarkPoolCollector()
    
    try:
        # Test the darkpool endpoint directly
        endpoint = f"{UW_BASE_URL}{DARKPOOL_RECENT_ENDPOINT}"
        response = collector._make_request(endpoint)
        
        if response and collector._validate_response(response):
            logger.info("Successfully connected to API and received valid data")
            return True
        else:
            logger.error("Failed to get valid response from API")
            return False
            
    except Exception as e:
        logger.error(f"API connection test failed: {str(e)}")
        return False

def test_data_processing():
    """Test data processing functionality"""
    logger.info("Testing data processing...")
    collector = DarkPoolCollector()
    
    # Create sample trade data
    sample_trades = [
        {
            "tracking_id": "12345",
            "symbol": "AAPL",
            "price": "150.00",
            "size": "1000",
            "executed_at": datetime.now().isoformat(),
            "nbbo_ask": "150.10",
            "nbbo_bid": "149.90",
            "sale_cond_codes": "@",
            "market_center": "L"
        },
        {
            "tracking_id": "12346",
            "symbol": "MSFT",
            "price": "280.00",
            "size": "500",
            "executed_at": datetime.now().isoformat(),
            "nbbo_ask": "280.20",
            "nbbo_bid": "279.80",
            "sale_cond_codes": "@",
            "market_center": "L"
        }
    ]
    
    try:
        # Process the sample trades
        trades_df = collector._process_trades(sample_trades)
        
        # Verify the processed data
        if not isinstance(trades_df, pd.DataFrame):
            logger.error("Processing failed: output is not a DataFrame")
            return False
            
        required_columns = [
            'tracking_id', 'symbol', 'price', 'size', 'executed_at',
            'nbbo_ask', 'nbbo_bid', 'sale_cond_codes', 'market_center',
            'premium'
        ]
        
        missing_columns = [col for col in required_columns if col not in trades_df.columns]
        if missing_columns:
            logger.error(f"Missing columns in processed data: {missing_columns}")
            return False
            
        logger.info("Data processing test passed")
        return True
        
    except Exception as e:
        logger.error(f"Data processing test failed: {str(e)}")
        return False

def test_save_to_db():
    """Test saving data to database"""
    logger.info("Testing database save functionality...")
    collector = DarkPoolCollector()
    
    # Create test data
    test_data = pd.DataFrame({
        'tracking_id': ['TEST123'],
        'symbol': ['TEST'],
        'price': [100.0],
        'size': [100],
        'executed_at': [datetime.now()],
        'nbbo_ask': [100.1],
        'nbbo_bid': [99.9],
        'market_center': ['L'],
        'sale_cond_codes': ['@'],
        'premium': [10000.0],
        'collection_time': [datetime.now()]
    })
    
    try:
        # Connect to database
        collector.connect_db()
        
        # Create cursor
        with collector.db_conn.cursor() as cur:
            # Create schema if it doesn't exist
            cur.execute("CREATE SCHEMA IF NOT EXISTS trading;")
            
            # Create table if it doesn't exist
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trading.darkpool_trades (
                    id SERIAL PRIMARY KEY,
                    tracking_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price NUMERIC NOT NULL,
                    size INTEGER NOT NULL,
                    executed_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    sale_cond_codes TEXT NOT NULL,
                    market_center TEXT NOT NULL,
                    nbbo_ask NUMERIC NOT NULL,
                    nbbo_bid NUMERIC NOT NULL,
                    premium NUMERIC NOT NULL,
                    collection_time TIMESTAMP WITH TIME ZONE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                
                -- Create indexes if they don't exist
                CREATE INDEX IF NOT EXISTS idx_darkpool_trades_tracking_id ON trading.darkpool_trades(tracking_id);
            """)
            collector.db_conn.commit()
        
            # Prepare data for insertion
            columns = [
                'tracking_id', 'symbol', 'price', 'size', 'executed_at',
                'nbbo_ask', 'nbbo_bid', 'sale_cond_codes', 'market_center',
                'premium', 'collection_time'
            ]
            values = [tuple(row) for row in test_data[columns].values]
            
            # Insert trades using execute_values for better performance
            execute_values(
                cur,
                """
                INSERT INTO trading.darkpool_trades (
                    tracking_id, symbol, price, size, executed_at,
                    nbbo_ask, nbbo_bid, sale_cond_codes, market_center,
                    premium, collection_time
                ) VALUES %s
                ON CONFLICT (tracking_id) DO NOTHING
                """,
                values
            )
            collector.db_conn.commit()
            
            # Verify the data was saved
            cur.execute("""
                SELECT COUNT(*) 
                FROM trading.darkpool_trades 
                WHERE tracking_id = 'TEST123'
                AND executed_at >= NOW() - INTERVAL '1 minute'
            """)
            count = cur.fetchone()[0]
            
        if count > 0:
            logger.info("Successfully saved and verified test data in database")
            
            # Clean up test data
            with collector.db_conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM trading.darkpool_trades 
                    WHERE tracking_id = 'TEST123'
                """)
                collector.db_conn.commit()
                
            return True
        else:
            logger.error("Failed to verify test data in database")
            return False
            
    except Exception as e:
        logger.error(f"Database save test failed: {str(e)}")
        return False

def run_all_tests():
    """Run all tests and return overall status"""
    tests = [
        ("Environment Variables", test_env_vars),
        ("Database Connection", test_db_connection),
        ("API Connection", test_api_connection),
        ("Data Processing", test_data_processing),
        ("Database Save", test_save_to_db)
    ]
    
    results = []
    for test_name, test_func in tests:
        logger.info(f"\n{'='*50}\nRunning {test_name} test...")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"Test {test_name} failed with error: {str(e)}")
            results.append((test_name, False))
    
    # Print summary
    logger.info("\n" + "="*50)
    logger.info("Test Summary:")
    logger.info("="*50)
    
    all_passed = True
    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        logger.info(f"{test_name}: {status}")
        if not result:
            all_passed = False
    
    logger.info("="*50)
    logger.info(f"Overall Status: {'PASSED' if all_passed else 'FAILED'}")
    logger.info("="*50)
    
    return all_passed

if __name__ == "__main__":
    run_all_tests() 