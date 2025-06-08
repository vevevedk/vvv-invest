#!/usr/bin/env python3

import os
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from collectors.economic.economic_collector import EconomicCollector
from config.db_config import get_db_config

def test_economic_collector():
    """Test the economic collector functionality."""
    try:
        # Initialize collector
        print("Initializing economic collector...")
        collector = EconomicCollector()
        
        # Test API connection and data retrieval
        print("\nTesting API connection and data retrieval...")
        events = collector.get_economic_calendar()
        if not events:
            print("❌ Failed to retrieve economic events from API")
            return
        
        print(f"✅ Successfully retrieved {len(events)} economic events")
        
        # Print sample of events
        print("\nSample of retrieved events:")
        for event in events[:3]:  # Show first 3 events
            print(f"- {event['event']} ({event['type']}) at {event['time']}")
        
        # Test data processing
        print("\nTesting data processing...")
        df = collector._process_event_data(events)
        if df.empty:
            print("❌ Failed to process economic events data")
            return
        
        print(f"✅ Successfully processed {len(df)} events")
        print("\nProcessed data sample:")
        print(df.head())
        
        # Test database operations
        print("\nTesting database operations...")
        num_saved = collector.save_to_database(df)
        if num_saved == 0:
            print("❌ Failed to save events to database")
            return
        
        print(f"✅ Successfully saved {num_saved} events to database")
        
        # Test full collection process
        print("\nTesting full collection process...")
        num_collected = collector.collect()
        if num_collected == 0:
            print("❌ Failed to complete collection process")
            return
        
        print(f"✅ Successfully completed collection process. Collected {num_collected} events")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        raise

if __name__ == "__main__":
    test_economic_collector() 