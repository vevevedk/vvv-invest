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