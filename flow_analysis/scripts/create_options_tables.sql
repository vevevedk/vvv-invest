-- Create options flow tables if they don't exist
CREATE TABLE IF NOT EXISTS trading.options_flow_signals (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP WITH TIME ZONE,
    expiry DATE,
    strike DECIMAL,
    option_type VARCHAR(4),  -- 'CALL' or 'PUT'
    premium DECIMAL,
    contract_size INTEGER,
    implied_volatility DECIMAL,
    delta DECIMAL,
    underlying_price DECIMAL,
    is_significant BOOLEAN,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Aggregated sentiment metrics table
CREATE TABLE IF NOT EXISTS trading.market_sentiment (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    timestamp TIMESTAMP WITH TIME ZONE,
    interval VARCHAR(10),  -- '5min', '1hour', '1day'
    call_volume INTEGER,
    put_volume INTEGER,
    call_premium DECIMAL,
    put_premium DECIMAL,
    net_delta DECIMAL,
    avg_iv DECIMAL,
    bullish_flow DECIMAL,
    bearish_flow DECIMAL,
    sentiment_score DECIMAL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_flow_signals_lookup 
ON trading.options_flow_signals(symbol, timestamp);

CREATE INDEX IF NOT EXISTS idx_flow_signals_collection 
ON trading.options_flow_signals(collected_at);

CREATE INDEX IF NOT EXISTS idx_market_sentiment_lookup 
ON trading.market_sentiment(symbol, timestamp, interval);

-- Add comments for documentation
COMMENT ON TABLE trading.options_flow_signals IS 'Stores individual options flow signals with premium filtering';
COMMENT ON TABLE trading.market_sentiment IS 'Stores aggregated market sentiment metrics at various intervals';

-- Grant necessary permissions
GRANT SELECT, INSERT ON trading.options_flow_signals TO collector;
GRANT SELECT, INSERT ON trading.market_sentiment TO collector;
GRANT USAGE ON SEQUENCE trading.options_flow_signals_id_seq TO collector;
GRANT USAGE ON SEQUENCE trading.market_sentiment_id_seq TO collector; 