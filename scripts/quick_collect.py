#!/usr/bin/env python3

import os
import sys
import subprocess
import logging
import argparse
from pathlib import Path

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run collectors with specified time period')
    parser.add_argument(
        '--period', 
        type=int, 
        required=True,
        help='Time period for backfill'
    )
    parser.add_argument(
        '--unit', 
        choices=['days', 'hours'],
        default='days',
        help='Time unit (days or hours)'
    )
    return parser.parse_args()

def run_collectors(period: int, unit: str):
    """Run both collectors for the specified time period."""
    try:
        # Convert days to hours for dark pool collector
        darkpool_period = period * 24 if unit == 'days' else period
        
        # Run dark pool collector
        logger.info(f"Running dark pool collector for last {period} {unit} ({darkpool_period} hours)...")
        subprocess.run([
            sys.executable, 
            "collectors/darkpool/darkpool_collector.py",
            "--backfill",
            "--hours", str(darkpool_period)
        ], check=True, env=dict(os.environ, PYTHONPATH=project_root))
        
        # Run news collector
        logger.info(f"\nRunning news collector for last {period} {unit}...")
        subprocess.run([
            sys.executable, 
            "collectors/news/newscollector.py",
            "--backfill",
            f"--{unit}", str(period)
        ], check=True, env=dict(os.environ, PYTHONPATH=project_root))
        
        logger.info(f"\n{period}-{unit} backfill completed successfully!")
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Collection failed: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    args = parse_args()
    run_collectors(args.period, args.unit) 