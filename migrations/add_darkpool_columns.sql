-- Migration: Add missing columns to trading.darkpool_trades for Unusual Whales API compatibility
ALTER TABLE trading.darkpool_trades
    ADD COLUMN IF NOT EXISTS nbbo_ask_quantity INTEGER,
    ADD COLUMN IF NOT EXISTS nbbo_bid_quantity INTEGER,
    ADD COLUMN IF NOT EXISTS ext_hour_sold_codes TEXT,
    ADD COLUMN IF NOT EXISTS trade_code TEXT,
    ADD COLUMN IF NOT EXISTS trade_settlement TEXT; 