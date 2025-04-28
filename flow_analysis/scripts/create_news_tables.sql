-- Create news headlines table
CREATE TABLE IF NOT EXISTS trading.news_headlines (
    id SERIAL PRIMARY KEY,
    headline TEXT,
    source VARCHAR(100),
    published_at TIMESTAMP WITH TIME ZONE,
    symbols TEXT[],
    sentiment VARCHAR(20),
    is_major BOOLEAN,
    tags TEXT[],
    meta JSONB,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create news sentiment metrics table
CREATE TABLE IF NOT EXISTS trading.news_sentiment (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP WITH TIME ZONE,
    interval VARCHAR(10),  -- '5min', '1hour', '1day'
    positive_count INTEGER,
    negative_count INTEGER,
    neutral_count INTEGER,
    major_news_count INTEGER,
    sentiment_score DECIMAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_news_headlines_lookup ON trading.news_headlines(symbols, published_at);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_lookup ON trading.news_sentiment(symbol, timestamp, interval);

-- Add unique constraint to prevent duplicate headlines
ALTER TABLE trading.news_headlines ADD CONSTRAINT unique_headline 
UNIQUE (headline, published_at, source);

-- Note: Removed foreign key constraint as it's not appropriate for this relationship
-- The news_sentiment table aggregates data and doesn't need a direct foreign key 