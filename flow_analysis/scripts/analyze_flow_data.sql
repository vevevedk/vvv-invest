-- Analyze options flow data collection for 2025-04-28
-- All times in EST/EDT (US/Eastern)

-- Count of flow signals by hour
WITH hourly_stats AS (
    SELECT 
        date_trunc('hour', timestamp) as hour,
        COUNT(*) as signal_count,
        COUNT(DISTINCT symbol) as unique_symbols,
        SUM(CASE WHEN option_type = 'call' THEN 1 ELSE 0 END) as call_count,
        SUM(CASE WHEN option_type = 'put' THEN 1 ELSE 0 END) as put_count,
        SUM(premium) as total_premium
    FROM flow_analysis.options_flow_signals
    WHERE timestamp >= '2025-04-28'::date AT TIME ZONE 'US/Eastern'
        AND timestamp < '2025-04-29'::date AT TIME ZONE 'US/Eastern'
    GROUP BY date_trunc('hour', timestamp)
    ORDER BY hour
),
-- Check for collection gaps (periods > 10 minutes without data)
time_gaps AS (
    SELECT 
        timestamp as gap_start,
        lead(timestamp) OVER (ORDER BY timestamp) as gap_end,
        EXTRACT(EPOCH FROM (lead(timestamp) OVER (ORDER BY timestamp) - timestamp)) / 60 as gap_minutes
    FROM flow_analysis.options_flow_signals
    WHERE timestamp >= '2025-04-28'::date AT TIME ZONE 'US/Eastern'
        AND timestamp < '2025-04-29'::date AT TIME ZONE 'US/Eastern'
    HAVING EXTRACT(EPOCH FROM (lead(timestamp) OVER (ORDER BY timestamp) - timestamp)) / 60 > 10
)

-- Output hourly statistics
SELECT 
    hour AT TIME ZONE 'US/Eastern' as hour_est,
    signal_count,
    unique_symbols,
    call_count,
    put_count,
    total_premium,
    ROUND(call_count::numeric / NULLIF(put_count, 0), 2) as call_put_ratio
FROM hourly_stats
ORDER BY hour;

-- Output collection gaps
SELECT 
    gap_start AT TIME ZONE 'US/Eastern' as gap_start_est,
    gap_end AT TIME ZONE 'US/Eastern' as gap_end_est,
    ROUND(gap_minutes::numeric, 1) as gap_minutes
FROM time_gaps
ORDER BY gap_start;

-- Summary statistics
SELECT 
    COUNT(*) as total_signals,
    COUNT(DISTINCT symbol) as total_symbols,
    SUM(CASE WHEN option_type = 'call' THEN 1 ELSE 0 END) as total_calls,
    SUM(CASE WHEN option_type = 'put' THEN 1 ELSE 0 END) as total_puts,
    SUM(premium) as total_premium,
    MIN(timestamp) AT TIME ZONE 'US/Eastern' as first_signal,
    MAX(timestamp) AT TIME ZONE 'US/Eastern' as last_signal
FROM flow_analysis.options_flow_signals
WHERE timestamp >= '2025-04-28'::date AT TIME ZONE 'US/Eastern'
    AND timestamp < '2025-04-29'::date AT TIME ZONE 'US/Eastern';

-- Top 10 symbols by flow count
SELECT 
    symbol,
    COUNT(*) as signal_count,
    SUM(CASE WHEN option_type = 'call' THEN 1 ELSE 0 END) as calls,
    SUM(CASE WHEN option_type = 'put' THEN 1 ELSE 0 END) as puts,
    SUM(premium) as total_premium
FROM flow_analysis.options_flow_signals
WHERE timestamp >= '2025-04-28'::date AT TIME ZONE 'US/Eastern'
    AND timestamp < '2025-04-29'::date AT TIME ZONE 'US/Eastern'
GROUP BY symbol
ORDER BY signal_count DESC
LIMIT 10; 