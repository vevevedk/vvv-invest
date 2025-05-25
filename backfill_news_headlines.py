import os
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime, timedelta
import pytz
import random
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv('.env.prod')

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSLMODE', 'require')
}

# Common tags for news headlines
NEWS_TAGS = [
    'earnings', 'tech', 'finance', 'market', 'economy', 'stocks', 'bonds',
    'crypto', 'forex', 'commodities', 'real-estate', 'biotech', 'energy',
    'retail', 'automotive', 'healthcare', 'telecom', 'media', 'transportation'
]

def create_backup():
    """Create a backup of the current news_headlines table."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                backup_table = f"news_headlines_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                logger.info(f"Creating backup table: {backup_table}")
                cur.execute(f"""
                    CREATE TABLE trading.{backup_table} AS 
                    SELECT * FROM trading.news_headlines;
                """)
                conn.commit()
                logger.info("Backup created successfully")
                return backup_table
    except Exception as e:
        logger.error(f"Error creating backup: {str(e)}")
        raise

def flush_table():
    """Flush the news_headlines table."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                logger.info("Flushing news_headlines table...")
                cur.execute("TRUNCATE TABLE trading.news_headlines RESTART IDENTITY CASCADE;")
                conn.commit()
                logger.info("Table flushed successfully")
    except Exception as e:
        logger.error(f"Error flushing table: {str(e)}")
        raise

def generate_tags(headline):
    """Generate relevant tags based on the headline content."""
    tags = set()
    headline_lower = headline.lower()
    
    # Add tags based on keywords in the headline
    for tag in NEWS_TAGS:
        if tag.replace('-', ' ') in headline_lower:
            tags.add(tag)
    
    # Add source-specific tags
    if 'earnings' in headline_lower or 'reports' in headline_lower:
        tags.add('earnings')
    if 'dividend' in headline_lower:
        tags.add('dividend')
    if 'acquisition' in headline_lower or 'merger' in headline_lower:
        tags.add('m&a')
    
    return list(tags) if tags else ['general']

def generate_meta(headline, source):
    """Generate metadata for the news headline."""
    return json.dumps({
        'source_type': 'press_release' if source in ['PR NewsWire', 'Business Wire', 'GlobeNewswire'] else 'news',
        'word_count': len(headline.split()),
        'has_numbers': any(c.isdigit() for c in headline),
        'has_currency': any(c in headline for c in ['$', '€', '£', '¥']),
        'has_percentage': '%' in headline
    })

def backfill_data(backup_table):
    """Backfill the news_headlines table with data from backup, adding sentiment and impact scores."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                logger.info("Starting data backfill...")
                
                # Get total count for progress tracking
                cur.execute(f"SELECT COUNT(*) FROM trading.{backup_table}")
                total_records = cur.fetchone()[0]
                logger.info(f"Total records to process: {total_records}")
                
                # Process in batches of 1000
                batch_size = 1000
                offset = 0
                processed = 0
                
                while True:
                    # Fetch batch of records
                    cur.execute(f"""
                        SELECT id, headline, content, published_at, source, symbols
                        FROM trading.{backup_table}
                        ORDER BY id
                        LIMIT {batch_size} OFFSET {offset}
                    """)
                    batch = cur.fetchall()
                    
                    if not batch:
                        break
                    
                    # Process each record
                    for record in batch:
                        id, headline, content, published_at, source, symbols = record
                        
                        # Generate sentiment score (-1.0 to 1.0)
                        sentiment = random.uniform(-1.0, 1.0)
                        
                        # Generate impact score (1.0 to 10.0)
                        impact_score = random.uniform(1.0, 10.0)
                        
                        # Determine if news is major based on impact score
                        is_major = impact_score >= 7.0
                        
                        # Generate tags and meta
                        tags = generate_tags(headline)
                        meta = generate_meta(headline, source)
                        
                        # Insert record with new scores and metadata
                        cur.execute("""
                            INSERT INTO trading.news_headlines 
                            (headline, content, published_at, source, symbols, 
                             sentiment, impact_score, is_major, tags, meta)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (headline, content, published_at, source, symbols,
                              sentiment, impact_score, is_major, tags, meta))
                    
                    processed += len(batch)
                    logger.info(f"Processed {processed}/{total_records} records")
                    
                    # Commit batch
                    conn.commit()
                    offset += batch_size
                
                logger.info("Data backfill completed successfully")
                
    except Exception as e:
        logger.error(f"Error during backfill: {str(e)}")
        raise

def main():
    try:
        # Create backup
        backup_table = create_backup()
        
        # Flush table
        flush_table()
        
        # Backfill data
        backfill_data(backup_table)
        
        logger.info("Backfill process completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    main() 