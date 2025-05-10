#!/usr/bin/env python3

"""
Script to validate news data in the local database
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import pytz
from sqlalchemy import create_engine, text

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from flow_analysis.config.db_config import DB_CONFIG

def validate_news_db():
    """Validate news data in the local database"""
    # Create database engine
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    try:
        with engine.connect() as conn:
            # Get total count of news items
            count_query = text("SELECT COUNT(*) FROM news_headlines")
            total_count = conn.execute(count_query).scalar()
            print(f"\nTotal news items in database: {total_count}")

            # Get recent news items (last 24 hours)
            recent_query = text("""
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
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY collected_at DESC
                LIMIT 5
            """)
            
            recent_news = pd.read_sql(recent_query, conn)
            
            if not recent_news.empty:
                print("\nMost recent news items:")
                print("=" * 80)
                for _, row in recent_news.iterrows():
                    print(f"\nHeadline: {row['headline']}")
                    print(f"Published: {row['published_at']}")
                    print(f"Source: {row['source']}")
                    print(f"URL: {row['url']}")
                    print(f"Symbols: {row['symbols']}")
                    print(f"Sentiment: {row['sentiment']:.2f}")
                    print(f"Impact Score: {row['impact_score']:.2f}")
                    print(f"Collected: {row['collected_at']}")
                    print("-" * 80)
            else:
                print("\nNo news items found in the last 24 hours")

            # Get sentiment distribution
            sentiment_query = text("""
                SELECT 
                    CASE 
                        WHEN sentiment > 0 THEN 'Positive'
                        WHEN sentiment < 0 THEN 'Negative'
                        ELSE 'Neutral'
                    END as sentiment_category,
                    COUNT(*) as count
                FROM news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                GROUP BY sentiment_category
                ORDER BY count DESC
            """)
            
            sentiment_dist = pd.read_sql(sentiment_query, conn)
            
            if not sentiment_dist.empty:
                print("\nSentiment distribution (last 24 hours):")
                print("=" * 80)
                for _, row in sentiment_dist.iterrows():
                    print(f"{row['sentiment_category']}: {row['count']} items")
            else:
                print("\nNo sentiment data available for the last 24 hours")

    except Exception as e:
        print(f"Error validating database: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    validate_news_db() 