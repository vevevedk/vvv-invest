-- Add unique constraint for market sentiment table
ALTER TABLE trading.market_sentiment
ADD CONSTRAINT unique_market_sentiment 
UNIQUE (symbol, interval, timestamp);

-- Verify the constraint was added
SELECT 
    tc.constraint_name, 
    tc.constraint_type,
    kcu.column_name
FROM 
    information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
WHERE 
    tc.table_schema = 'trading'
    AND tc.table_name = 'market_sentiment'
    AND tc.constraint_type = 'UNIQUE';
