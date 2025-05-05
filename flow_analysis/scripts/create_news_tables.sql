-- Create news headlines table if it doesn't exist
CREATE TABLE IF NOT EXISTS trading.news_headlines (
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

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_news_symbols ON trading.news_headlines USING GIN(symbols);
CREATE INDEX IF NOT EXISTS idx_news_published ON trading.news_headlines (published_at);
CREATE INDEX IF NOT EXISTS idx_news_collected ON trading.news_headlines (collected_at);

-- Add comments for documentation
COMMENT ON TABLE trading.news_headlines IS 'Stores news headlines with sentiment and impact analysis';
COMMENT ON COLUMN trading.news_headlines.sentiment IS 'Sentiment score from -1.0 (negative) to 1.0 (positive)';
COMMENT ON COLUMN trading.news_headlines.impact_score IS 'Impact score from 1 (low) to 10 (high)';

-- Grant necessary permissions
GRANT SELECT, INSERT ON trading.news_headlines TO collector;
GRANT USAGE ON SEQUENCE trading.news_headlines_id_seq TO collector; 