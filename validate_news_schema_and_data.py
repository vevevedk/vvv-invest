import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Load production environment variables
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

def get_table_schema():
    """Get the current schema of the news_headlines table."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'trading'
                AND table_name = 'news_headlines'
                ORDER BY ordinal_position;
            """)
            return cur.fetchall()

def get_column_null_counts():
    """Get count of NULL values for each column in the last 24 hours."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    'headline' as column_name,
                    COUNT(*) FILTER (WHERE headline IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'content' as column_name,
                    COUNT(*) FILTER (WHERE content IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'published_at' as column_name,
                    COUNT(*) FILTER (WHERE published_at IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'source' as column_name,
                    COUNT(*) FILTER (WHERE source IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'symbols' as column_name,
                    COUNT(*) FILTER (WHERE symbols IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'sentiment' as column_name,
                    COUNT(*) FILTER (WHERE sentiment IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'impact_score' as column_name,
                    COUNT(*) FILTER (WHERE impact_score IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'is_major' as column_name,
                    COUNT(*) FILTER (WHERE is_major IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'tags' as column_name,
                    COUNT(*) FILTER (WHERE tags IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                UNION ALL
                SELECT 
                    'meta' as column_name,
                    COUNT(*) FILTER (WHERE meta IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours';
            """)
            return cur.fetchall()

def get_recent_news_sample():
    """Get a sample of recent news to verify data quality."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '24 hours'
                ORDER BY collected_at DESC
                LIMIT 5;
            """)
            columns = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)

def get_news_counts_per_source():
    """Get count of news items per source for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source, COUNT(*) as news_count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '7 days'
                GROUP BY source
                ORDER BY news_count DESC;
            """)
            return cur.fetchall()

def get_sentiment_distribution():
    """Get distribution of sentiment scores for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH sentiment_categories AS (
                    SELECT 
                        CASE 
                            WHEN sentiment < -0.5 THEN 'Very Negative'
                            WHEN sentiment < 0 THEN 'Negative'
                            WHEN sentiment = 0 THEN 'Neutral'
                            WHEN sentiment <= 0.5 THEN 'Positive'
                            ELSE 'Very Positive'
                        END as category,
                        COUNT(*) as count
                    FROM trading.news_headlines
                    WHERE collected_at >= NOW() - INTERVAL '7 days'
                    GROUP BY category
                )
                SELECT category, count
                FROM sentiment_categories
                ORDER BY 
                    CASE category
                        WHEN 'Very Negative' THEN 1
                        WHEN 'Negative' THEN 2
                        WHEN 'Neutral' THEN 3
                        WHEN 'Positive' THEN 4
                        WHEN 'Very Positive' THEN 5
                    END;
            """)
            return cur.fetchall()

def get_impact_score_distribution():
    """Get distribution of impact scores for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH impact_categories AS (
                    SELECT 
                        CASE 
                            WHEN impact_score <= 2 THEN 'Very Low'
                            WHEN impact_score <= 4 THEN 'Low'
                            WHEN impact_score <= 6 THEN 'Medium'
                            WHEN impact_score <= 8 THEN 'High'
                            ELSE 'Very High'
                        END as category,
                        COUNT(*) as count
                    FROM trading.news_headlines
                    WHERE collected_at >= NOW() - INTERVAL '7 days'
                    GROUP BY category
                )
                SELECT category, count
                FROM impact_categories
                ORDER BY 
                    CASE category
                        WHEN 'Very Low' THEN 1
                        WHEN 'Low' THEN 2
                        WHEN 'Medium' THEN 3
                        WHEN 'High' THEN 4
                        WHEN 'Very High' THEN 5
                    END;
            """)
            return cur.fetchall()

def get_major_news_distribution():
    """Get distribution of major vs non-major news for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    CASE WHEN is_major THEN 'Major' ELSE 'Non-Major' END as category,
                    COUNT(*) as count
                FROM trading.news_headlines
                WHERE collected_at >= NOW() - INTERVAL '7 days'
                GROUP BY category
                ORDER BY category;
            """)
            return cur.fetchall()

def get_tags_distribution():
    """Get distribution of most common tags for the last 7 days."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                WITH tag_counts AS (
                    SELECT unnest(tags) as tag, COUNT(*) as count
                    FROM trading.news_headlines
                    WHERE collected_at >= NOW() - INTERVAL '7 days'
                    GROUP BY tag
                )
                SELECT tag, count
                FROM tag_counts
                ORDER BY count DESC
                LIMIT 10;
            """)
            return cur.fetchall()

def main():
    print("\n=== News Headlines Schema and Data Validation ===\n")
    
    # Check schema
    print("1. Current Table Schema:")
    print("-" * 80)
    schema = get_table_schema()
    for col in schema:
        print(f"Column: {col[0]:<20} Type: {col[1]:<15} Nullable: {col[2]:<5} Default: {col[3]}")
    
    # Check NULL values
    print("\n2. NULL Value Analysis (Last 24 Hours):")
    print("-" * 80)
    null_counts = get_column_null_counts()
    for col, null_count, total_count in null_counts:
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        print(f"Column: {col:<20} NULL Count: {null_count:<5} Total: {total_count:<5} NULL %: {null_percentage:.1f}%")
    
    # Show sample data
    print("\n3. Recent News Sample (Last 24 Hours):")
    print("-" * 80)
    sample = get_recent_news_sample()
    print(sample.to_string())

    # Print news counts per source for last 7 days
    print("\n4. News Counts Per Source (Last 7 Days):")
    print("-" * 80)
    counts = get_news_counts_per_source()
    for source, count in counts:
        print(f"Source: {source:<30} Count: {count}")

    # Print sentiment distribution
    print("\n5. Sentiment Distribution (Last 7 Days):")
    print("-" * 80)
    sentiment_dist = get_sentiment_distribution()
    for category, count in sentiment_dist:
        print(f"Category: {category:<15} Count: {count}")

    # Print impact score distribution
    print("\n6. Impact Score Distribution (Last 7 Days):")
    print("-" * 80)
    impact_dist = get_impact_score_distribution()
    for category, count in impact_dist:
        print(f"Category: {category:<15} Count: {count}")

    # Print major news distribution
    print("\n7. Major vs Non-Major News Distribution (Last 7 Days):")
    print("-" * 80)
    major_dist = get_major_news_distribution()
    for category, count in major_dist:
        print(f"Category: {category:<15} Count: {count}")

    # Print top tags distribution
    print("\n8. Top 10 Tags Distribution (Last 7 Days):")
    print("-" * 80)
    tags_dist = get_tags_distribution()
    for tag, count in tags_dist:
        print(f"Tag: {tag:<20} Count: {count}")

if __name__ == "__main__":
    main() 