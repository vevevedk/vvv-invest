"""
Test script for environment configuration and Celery setup.
Run this script to verify the configuration before deploying to production.
"""

import os
import sys
import logging
from pathlib import Path
import psycopg2
from celery import Celery

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_env_file():
    """Test environment file loading."""
    logger.info("Testing environment file loading...")
    try:
        from flow_analysis.config.env_config import ENV_FILE, ENV_PATH
        logger.info(f"Environment file: {ENV_FILE}")
        logger.info(f"Environment file exists: {ENV_PATH.exists()}")
        return True
    except Exception as e:
        logger.error(f"Environment file test failed: {str(e)}")
        return False

def test_api_token():
    """Test API token configuration."""
    logger.info("Testing API token configuration...")
    try:
        from flow_analysis.config.env_config import UW_API_TOKEN
        logger.info("API token is configured")
        return True
    except Exception as e:
        logger.error(f"API token test failed: {str(e)}")
        return False

def test_database_connection():
    """Test database connection."""
    logger.info("Testing database connection...")
    try:
        from flow_analysis.config.env_config import DB_CONFIG
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

def test_celery_config():
    """Test Celery configuration."""
    logger.info("Testing Celery configuration...")
    try:
        from flow_analysis.config.env_config import CELERY_CONFIG
        from celery_app import news_app, darkpool_app
        
        # Test news app configuration
        logger.info("Testing news app configuration...")
        news_app.conf.update(CELERY_CONFIG)
        logger.info("News app configuration successful")
        
        # Test darkpool app configuration
        logger.info("Testing darkpool app configuration...")
        darkpool_app.conf.update(CELERY_CONFIG)
        logger.info("Darkpool app configuration successful")
        
        return True
    except Exception as e:
        logger.error(f"Celery configuration test failed: {str(e)}")
        return False

def test_manual_task():
    """Test manual task execution."""
    logger.info("Testing manual task execution...")
    try:
        from celery_app import news_app
        result = news_app.send_task('celery_app.run_news_collector_task')
        logger.info(f"Task sent with ID: {result.id}")
        return True
    except Exception as e:
        logger.error(f"Manual task test failed: {str(e)}")
        return False

def main():
    """Run all tests."""
    tests = [
        ("Environment File", test_env_file),
        ("API Token", test_api_token),
        ("Database Connection", test_database_connection),
        ("Celery Configuration", test_celery_config),
        ("Manual Task", test_manual_task)
    ]
    
    results = []
    for name, test_func in tests:
        logger.info(f"\nRunning {name} test...")
        success = test_func()
        results.append((name, success))
    
    # Print summary
    logger.info("\nTest Summary:")
    logger.info("-" * 50)
    for name, success in results:
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"{name}: {status}")
    
    # Return overall success
    return all(success for _, success in results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 