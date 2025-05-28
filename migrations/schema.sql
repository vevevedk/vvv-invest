-- Create schema for trading data
CREATE SCHEMA IF NOT EXISTS trading;

-- Create darkpool trades table
CREATE TABLE trading.darkpool_trades (
    id SERIAL PRIMARY KEY,
    tracking_id VARCHAR(255) UNIQUE,
    symbol VARCHAR(10),
    size DECIMAL,
    price DECIMAL,
    volume DECIMAL,
    premium DECIMAL,
    executed_at TIMESTAMP,
    nbbo_ask DECIMAL,
    nbbo_bid DECIMAL,
    market_center VARCHAR(50),
    sale_cond_codes VARCHAR(50),
    collection_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for common queries
CREATE INDEX idx_darkpool_trades_symbol ON trading.darkpool_trades(symbol);
CREATE INDEX idx_darkpool_trades_executed_at ON trading.darkpool_trades(executed_at);
CREATE INDEX idx_darkpool_trades_size ON trading.darkpool_trades(size);
CREATE INDEX idx_darkpool_trades_price ON trading.darkpool_trades(price);

-- Add comment to table
COMMENT ON TABLE trading.darkpool_trades IS 'Stores dark pool trade data collected from Unusual Whales API';

-- Options Flow Tables

-- Main options flow table
CREATE TABLE IF NOT EXISTS trading.options_flow (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    strike DECIMAL,
    expiry DATE,
    flow_type VARCHAR(20),
    premium DECIMAL,
    contract_size INTEGER,
    iv_rank DECIMAL,
    collected_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Price levels and market metrics
CREATE TABLE IF NOT EXISTS trading.market_metrics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    metric_type VARCHAR(50),
    value DECIMAL,
    timestamp TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for options flow
CREATE INDEX IF NOT EXISTS idx_options_flow_symbol 
ON trading.options_flow(symbol);

CREATE INDEX IF NOT EXISTS idx_options_flow_timestamp 
ON trading.options_flow(collected_at);

CREATE INDEX IF NOT EXISTS idx_options_flow_lookup 
ON trading.options_flow(symbol, expiry, collected_at);

-- Indexes for market metrics
CREATE INDEX IF NOT EXISTS idx_market_metrics_lookup 
ON trading.market_metrics(symbol, metric_type, timestamp);

-- Comments
COMMENT ON TABLE trading.options_flow IS 'Stores options flow data from Unusual Whales API';
COMMENT ON TABLE trading.market_metrics IS 'Stores price levels, IV ranks, and other market metrics';

-- Permissions
GRANT SELECT, INSERT ON trading.options_flow TO collector;
GRANT SELECT, INSERT ON trading.market_metrics TO collector;
GRANT USAGE ON SEQUENCE trading.options_flow_id_seq TO collector;
GRANT USAGE ON SEQUENCE trading.market_metrics_id_seq TO collector; 