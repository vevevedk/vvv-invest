-- Add unique constraint for ON CONFLICT clause
ALTER TABLE trading.options_flow_signals
ADD CONSTRAINT unique_flow_signal 
UNIQUE (symbol, timestamp, strike, option_type);

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
    AND tc.table_name = 'options_flow_signals'
    AND tc.constraint_type = 'UNIQUE';
