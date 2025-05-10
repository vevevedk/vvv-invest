#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import pytz
from pathlib import Path

from flow_analysis.config.db_config import DB_CONFIG

def export_news_to_csv():
    """Export news data from database to CSV file"""
    # Create SQLAlchemy engine
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    
    # Query to get all news items
    query = """
    SELECT 
        headline,
        published_at,
        source,
        url,
        symbols,
        sentiment,
        impact_score,
        collected_at
    FROM news_headlines
    ORDER BY published_at DESC
    """
    
    # Read data into DataFrame
    df = pd.read_sql(query, engine)
    
    # Create exports directory if it doesn't exist
    export_dir = Path("exports")
    export_dir.mkdir(exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = export_dir / f"news_data_{timestamp}.csv"
    
    # Export to CSV
    df.to_csv(filename, index=False)
    print(f"Exported {len(df)} news items to {filename}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total news items: {len(df)}")
    print(f"Date range: {df['published_at'].min()} to {df['published_at'].max()}")
    print(f"Number of sources: {df['source'].nunique()}")
    print("\nTop 5 sources:")
    print(df['source'].value_counts().head())
    print("\nSentiment distribution:")
    print(df['sentiment'].value_counts().sort_index())

if __name__ == "__main__":
    export_news_to_csv() 