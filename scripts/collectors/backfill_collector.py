import os
import psycopg2
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

# Load environment variables from .env
load_dotenv('.env')

# Read database connection details from environment variables
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Dummy headlines for backfill
dummy_headlines = [
    "Market Rally Continues as Tech Stocks Surge",
    "Federal Reserve Signals Potential Rate Cut",
    "Oil Prices Drop Amid Global Supply Concerns",
    "New IPO Filing Shows Strong Investor Interest",
    "Earnings Report Exceeds Analyst Expectations",
    "Cryptocurrency Market Faces Volatility",
    "Economic Data Points to Slower Growth",
    "Merger Announcement Boosts Sector Confidence",
    "Regulatory Changes Impact Financial Markets",
    "Global Trade Tensions Affect Market Sentiment",
    "Startup Secures Major Funding Round",
    "Housing Market Shows Signs of Cooling",
    "Automotive Industry Faces Supply Chain Challenges",
    "Renewable Energy Stocks Gain Momentum",
    "Banking Sector Reports Strong Quarterly Results",
    "Retail Sales Data Surprises Analysts",
    "Tech Giant Unveils New Product Line",
    "Inflation Concerns Prompt Market Adjustments",
    "Healthcare Sector Sees Increased M&A Activity",
    "Transportation Stocks Hit by Rising Fuel Costs",
    "Agricultural Commodities Face Weather Impact",
    "Entertainment Industry Adapts to Digital Shift",
    "Real Estate Market Shows Regional Variations",
    "Consumer Confidence Index Rises Unexpectedly"
]

# Connect to the database and insert dummy headlines for the last 24 hours
try:
    conn = psycopg2.connect(**db_config)
    cursor = conn.cursor()
    now = datetime.now(timezone.utc)
    for i in range(24):
        published_at = now - timedelta(hours=i)
        data = {
            'headline': dummy_headlines[i % len(dummy_headlines)],
            'source': 'Backfill',
            'published_at': published_at,
            'sentiment': 0.0,
            'impact_score': 0.0,
            'is_major': False,
            'symbols': [],
            'tags': [],
            'meta': json.dumps({}),
            'collected_at': published_at
        }
        cursor.execute("""
            INSERT INTO trading.news_headlines 
            (headline, source, published_at, sentiment, impact_score, is_major, symbols, tags, meta, collected_at)
            VALUES (%(headline)s, %(source)s, %(published_at)s, %(sentiment)s, %(impact_score)s, %(is_major)s, %(symbols)s, %(tags)s, %(meta)s, %(collected_at)s)
        """, data)
    conn.commit()
    print("Backfill completed successfully.")
except Exception as e:
    print(f"Error during backfill: {e}")
finally:
    if conn:
        conn.close() 