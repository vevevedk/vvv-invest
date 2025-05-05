"""
Database Configuration
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'sslmode': os.getenv('DB_SSL_MODE', 'disable')  # Default to disable for local development
}

# Schema and table configuration
SCHEMA_NAME = 'trading'
TABLE_NAME = 'darkpool_trades'

# Table Names
DARKPOOL_TRADES_TABLE = "darkpool_trades"
OPTIONS_FLOW_TABLE = "options_flow_signals"
MARKET_SENTIMENT_TABLE = "market_sentiment"
NEWS_HEADLINES_TABLE = "news_headlines"

# Schema Creation SQL
SCHEMA_CREATION_SQL = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
    
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{DARKPOOL_TRADES_TABLE} (
        tracking_id VARCHAR(50) PRIMARY KEY,
        symbol VARCHAR(10) NOT NULL,
        size INTEGER NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        volume INTEGER NOT NULL,
        premium DECIMAL(15,2) NOT NULL,
        executed_at TIMESTAMP NOT NULL,
        nbbo_ask DECIMAL(10,2),
        nbbo_bid DECIMAL(10,2),
        market_center VARCHAR(10),
        sale_cond_codes VARCHAR(10),
        collection_time TIMESTAMP NOT NULL
    );
    
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{OPTIONS_FLOW_TABLE} (
        symbol VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        expiry DATE NOT NULL,
        strike DECIMAL(10,2) NOT NULL,
        option_type VARCHAR(4) NOT NULL,
        premium DECIMAL(15,2) NOT NULL,
        contract_size INTEGER NOT NULL,
        implied_volatility DECIMAL(10,4),
        delta DECIMAL(10,4),
        underlying_price DECIMAL(10,2),
        is_significant BOOLEAN,
        PRIMARY KEY (symbol, timestamp, strike, option_type)
    );
    
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{MARKET_SENTIMENT_TABLE} (
        symbol VARCHAR(10) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        interval VARCHAR(10) NOT NULL,
        call_volume INTEGER,
        put_volume INTEGER,
        call_premium DECIMAL(15,2),
        put_premium DECIMAL(15,2),
        net_delta DECIMAL(15,2),
        avg_iv DECIMAL(10,4),
        bullish_flow DECIMAL(15,2),
        bearish_flow DECIMAL(15,2),
        sentiment_score DECIMAL(10,4),
        PRIMARY KEY (symbol, timestamp, interval)
    );
    
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{NEWS_HEADLINES_TABLE} (
        id SERIAL PRIMARY KEY,
        headline TEXT NOT NULL,
        source VARCHAR(100) NOT NULL,
        published_at TIMESTAMP NOT NULL,
        symbols TEXT[] NOT NULL,
        sentiment DECIMAL(10,4),
        impact_score INTEGER,
        collected_at TIMESTAMP NOT NULL,
        UNIQUE (headline, published_at)
    );
    
    CREATE INDEX IF NOT EXISTS idx_news_symbols ON {SCHEMA_NAME}.{NEWS_HEADLINES_TABLE} USING GIN(symbols);
    CREATE INDEX IF NOT EXISTS idx_news_published ON {SCHEMA_NAME}.{NEWS_HEADLINES_TABLE} (published_at);
    CREATE INDEX IF NOT EXISTS idx_news_collected ON {SCHEMA_NAME}.{NEWS_HEADLINES_TABLE} (collected_at);
""" 