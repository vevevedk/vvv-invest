-- Add new columns to options_flow table for enhanced sentiment analysis
ALTER TABLE trading.options_flow 
ADD COLUMN IF NOT EXISTS delta DECIMAL,
ADD COLUMN IF NOT EXISTS underlying_price DECIMAL,
ADD COLUMN IF NOT EXISTS is_significant BOOLEAN DEFAULT FALSE;

-- Add index for performance optimization
CREATE INDEX IF NOT EXISTS idx_options_flow_significant 
ON trading.options_flow(symbol, is_significant, collected_at);

-- Add comments for documentation
COMMENT ON COLUMN trading.options_flow.delta IS 'Delta value of the option, indicating directional exposure';
COMMENT ON COLUMN trading.options_flow.underlying_price IS 'Price of the underlying asset at time of trade';
COMMENT ON COLUMN trading.options_flow.is_significant IS 'Flag for significant trades based on premium threshold';

-- Verify the changes
SELECT 
    column_name, 
    data_type, 
    column_default,
    is_nullable
FROM information_schema.columns 
WHERE table_schema = 'trading' 
AND table_name = 'options_flow'
ORDER BY ordinal_position; 