import pandas as pd
from sqlalchemy import create_engine
import os
from datetime import datetime

# Database connection setup
DB_CONFIG = {
    'dbname': 'defaultdb',
    'user': 'doadmin',
    'password': 'AVNS_SrG4Bo3B7uCNEPONkE4',
    'host': 'vvv-trading-db-do-user-2110609-0.i.db.ondigitalocean.com',
    'port': '25060'
}

# Create database URL
DATABASE_URL = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

# Create engine with SSL required
engine = create_engine(
    DATABASE_URL,
    connect_args={
        'sslmode': 'require'
    }
)

# Query all dark pool trades with enhanced metrics
query = """
SELECT 
    t.*,
    date_trunc('hour', t.executed_at) as trade_hour,
    t.price - t.nbbo_bid as price_impact,
    (t.price - t.nbbo_bid) / t.nbbo_bid as price_impact_pct,
    CASE 
        WHEN t.size >= 10000 THEN 'Block Trade'
        WHEN t.premium >= 0.02 THEN 'High Premium'
        ELSE 'Regular'
    END as trade_type,
    count(*) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as trades_per_hour,
    sum(t.size) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as volume_per_hour
FROM trading.darkpool_trades t
ORDER BY t.executed_at DESC
"""

# Fetch trades
print("Fetching all dark pool trades...")
trades_df = pd.read_sql_query(query, engine)

# Convert timestamp columns
trades_df['executed_at'] = pd.to_datetime(trades_df['executed_at'])
trades_df['collection_time'] = pd.to_datetime(trades_df['collection_time'])
trades_df['trade_hour'] = pd.to_datetime(trades_df['trade_hour'])

# Create data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Generate filename with current timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
filename = f'data/darkpool_trades_all_{timestamp}.csv'

# Save to CSV
trades_df.to_csv(filename, index=False)
print(f"\nSaved {len(trades_df)} trades to {filename}")

# Print summary statistics
print("\nTrade summary by symbol:")
print(trades_df.groupby('symbol').agg({
    'size': ['count', 'sum', 'mean'],
    'premium': ['mean', 'max'],
    'price_impact_pct': 'mean'
}).round(2))

print("\nDate range of trades:")
print(f"Earliest trade: {trades_df['executed_at'].min()}")
print(f"Latest trade: {trades_df['executed_at'].max()}")
print(f"Total number of trades: {len(trades_df)}")
print(f"Total volume: {trades_df['size'].sum():,.0f}") 