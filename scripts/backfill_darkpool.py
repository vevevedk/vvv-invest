#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import argparse
from datetime import datetime, timedelta
import pytz

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from collectors.darkpool_collector import DarkPoolCollector

def main():
    """Run dark pool backfill for specified symbols and time range."""
    parser = argparse.ArgumentParser(description='Backfill dark pool trades')
    parser.add_argument('--symbols', nargs='+', default=['SPY', 'QQQ', 'GLD'],
                      help='List of symbols to backfill (default: SPY QQQ GLD)')
    parser.add_argument('--hours', type=int, default=24,
                      help='Number of hours to look back (default: 24)')
    args = parser.parse_args()
    
    print(f"\nStarting dark pool backfill for last {args.hours} hours")
    print(f"Symbols: {', '.join(args.symbols)}")
    print(f"Start time: {datetime.now(pytz.UTC) - timedelta(hours=args.hours)}")
    print(f"End time: {datetime.now(pytz.UTC)}")
    
    collector = DarkPoolCollector()
    results = collector.backfill_trades(args.symbols, args.hours)
    
    # Print summary
    print("\nBackfill Summary:")
    print("-" * 50)
    for symbol, count in results.items():
        status = f"{count} trades" if count >= 0 else "Failed"
        print(f"{symbol}: {status}")
    print("-" * 50)

if __name__ == "__main__":
    main() 