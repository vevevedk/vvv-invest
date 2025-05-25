import os
import psycopg2
from dotenv import load_dotenv
import logging
from datetime import datetime

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

def fix_news_schema():
    """Fix the schema of the news_headlines table."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Create backup of existing data
                logger.info("Creating backup of existing data...")
                backup_table = f"news_headlines_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                cur.execute(f"""
                    CREATE TABLE trading.{backup_table} AS 
                    SELECT * FROM trading.news_headlines;
                """)
                
                # Drop existing table
                logger.info("Dropping existing table...")
                cur.execute("DROP TABLE IF EXISTS trading.news_headlines CASCADE;")
                
                # Create new table with correct schema
                logger.info("Creating new table with correct schema...")
                cur.execute("""
                    CREATE TABLE trading.news_headlines (
                        id SERIAL PRIMARY KEY,
                        headline TEXT NOT NULL,
                        content TEXT,
                        published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                        source TEXT NOT NULL,
                        symbols TEXT[] NOT NULL,
                        sentiment DOUBLE PRECISION,
                        impact_score DOUBLE PRECISION,
                        is_major BOOLEAN DEFAULT FALSE,
                        tags TEXT[] DEFAULT '{}',
                        meta JSONB DEFAULT '{}',
                        collected_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (headline, published_at)
                    );
                """)
                
                # Create indexes
                logger.info("Creating indexes...")
                cur.execute("""
                    CREATE INDEX idx_news_symbols ON trading.news_headlines USING GIN(symbols);
                    CREATE INDEX idx_news_published ON trading.news_headlines (published_at);
                    CREATE INDEX idx_news_collected ON trading.news_headlines (collected_at);
                """)
                
                # Restore data with type conversion
                logger.info("Restoring data with type conversion...")
                cur.execute(f"""
                    INSERT INTO trading.news_headlines (
                        headline, content, published_at, source, 
                        symbols, sentiment, impact_score, collected_at
                    )
                    SELECT 
                        headline,
                        content,
                        published_at,
                        source,
                        symbols,
                        CASE 
                            WHEN sentiment::text = 'neutral' THEN 0.0
                            WHEN sentiment::text = 'positive' THEN 1.0
                            WHEN sentiment::text = 'negative' THEN -1.0
                            WHEN sentiment::text ~ '^[+-]?([0-9]*[.])?[0-9]+$' THEN sentiment::double precision
                            ELSE NULL
                        END as sentiment,
                        CASE 
                            WHEN impact_score::text ~ '^[+-]?([0-9]*[.])?[0-9]+$' THEN impact_score::double precision
                            WHEN impact_score::text = 'high' THEN 10.0
                            WHEN impact_score::text = 'medium' THEN 5.0
                            WHEN impact_score::text = 'low' THEN 1.0
                            ELSE NULL
                        END as impact_score,
                        collected_at
                    FROM (
                        SELECT DISTINCT ON (headline, published_at) *
                        FROM trading.{backup_table}
                        ORDER BY headline, published_at, id DESC
                    ) AS deduped_data;
                """)
                
                # Add comments
                logger.info("Adding table comments...")
                cur.execute("""
                    COMMENT ON TABLE trading.news_headlines IS 
                    'Stores news headlines with sentiment and impact analysis';
                    
                    COMMENT ON COLUMN trading.news_headlines.sentiment IS 
                    'Sentiment score from -1.0 (negative) to 1.0 (positive)';
                    
                    COMMENT ON COLUMN trading.news_headlines.impact_score IS 
                    'Impact score from 1.0 (low) to 10.0 (high)';
                """)
                
                # Grant permissions
                logger.info("Granting permissions...")
                cur.execute("""
                    GRANT SELECT, INSERT ON trading.news_headlines TO collector;
                    GRANT USAGE ON SEQUENCE trading.news_headlines_id_seq TO collector;
                """)
                
                conn.commit()
                logger.info("Schema migration completed successfully!")
                
                # Print summary
                cur.execute("SELECT COUNT(*) FROM trading.news_headlines;")
                count = cur.fetchone()[0]
                logger.info(f"Migrated {count} records successfully")
                
    except Exception as e:
        logger.error(f"Error during schema migration: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        raise

if __name__ == "__main__":
    fix_news_schema() 