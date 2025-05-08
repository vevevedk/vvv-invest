-- Create news headlines table
CREATE TABLE IF NOT EXISTS trading.news_headlines (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    source VARCHAR(100) NOT NULL,
    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
    symbols TEXT[] NOT NULL,
    sentiment DECIMAL(10,4),
    impact_score INTEGER,
    is_major BOOLEAN,
    tags TEXT[],
    meta JSONB,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (headline, published_at)
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

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_news_symbols ON trading.news_headlines USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_news_published ON trading.news_headlines (published_at);
CREATE INDEX IF NOT EXISTS idx_news_collected ON trading.news_headlines (collected_at);
CREATE INDEX IF NOT EXISTS idx_news_sentiment_lookup ON trading.news_sentiment(symbol, timestamp, interval);

-- Add comments for documentation
COMMENT ON TABLE trading.news_headlines IS 'Stores news headlines with sentiment and impact analysis';
COMMENT ON COLUMN trading.news_headlines.sentiment IS 'Sentiment score from -1.0 (negative) to 1.0 (positive)';
COMMENT ON COLUMN trading.news_headlines.impact_score IS 'Impact score from 1 (low) to 10 (high)';
COMMENT ON TABLE trading.news_sentiment IS 'Aggregated sentiment metrics for news headlines';

-- Grant necessary permissions
GRANT SELECT, INSERT ON trading.news_headlines TO collector;
GRANT SELECT, INSERT ON trading.news_sentiment TO collector;
GRANT USAGE ON SEQUENCE trading.news_headlines_id_seq TO collector;
GRANT USAGE ON SEQUENCE trading.news_sentiment_id_seq TO collector;
