import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime, timedelta
import sys
import time
from flow_analysis.config.db_config import get_db_config

# Database connection setup
DATABASE_URL = f"postgresql://{get_db_config()['user']}:{get_db_config()['password']}@{get_db_config()['host']}:{get_db_config()['port']}/{get_db_config()['dbname']}"
engine = create_engine(DATABASE_URL, connect_args={'sslmode': 'require'})

# Calculate timestamp for 7 days ago
seven_days_ago = datetime.now() - timedelta(days=7)

# Create data directory
os.makedirs('data', exist_ok=True)

# Generate filenames with current timestamp
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
darkpool_filename = f'data/darkpool_trades_7d_{timestamp}.csv'
options_filename = f'data/options_flow_7d_{timestamp}.csv'

print("Fetching dark pool trades from last 7 days...")
print("This might take some time depending on the database size...")

# Modified query for dark pool trades - last 7 days
darkpool_query = """
SELECT 
    t.*,
    date_trunc('hour', t.executed_at) as trade_hour,
    t.price - t.nbbo_bid as price_impact,
    CASE 
        WHEN t.nbbo_bid IS NULL OR t.nbbo_bid = 0 THEN NULL
        ELSE (t.price - t.nbbo_bid) / t.nbbo_bid
    END as price_impact_pct,
    CASE 
        WHEN t.size >= 10000 THEN 'Block Trade'
        WHEN t.premium >= 1000000 THEN 'High Premium'
        ELSE 'Regular'
    END as trade_type,
    count(*) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as trades_per_hour,
    sum(t.size) over (partition by t.symbol, date_trunc('hour', t.executed_at)) as volume_per_hour
FROM trading.darkpool_trades t
WHERE t.executed_at >= :seven_days_ago
ORDER BY t.executed_at DESC
"""

# Modified query for options flow - last 7 days
options_query = """
SELECT 
    f.*,
    date_trunc('hour', f.collected_at) as flow_hour,
    CASE 
        WHEN f.premium >= 1000000 THEN 'Whale'
        WHEN f.premium >= 100000 THEN 'Large'
        ELSE 'Regular'
    END as flow_size,
    count(*) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as flows_per_hour,
    sum(f.premium) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as premium_per_hour,
    sum(f.contract_size) over (partition by f.symbol, date_trunc('hour', f.collected_at)) as contracts_per_hour
FROM trading.options_flow f
WHERE f.collected_at >= :seven_days_ago
ORDER BY f.collected_at DESC
"""

def fetch_in_chunks(query, engine, filename, params=None, chunk_size=10000):
    """Fetch data in chunks to avoid memory issues with large datasets."""
    start_time = time.time()
    connection = engine.connect().execution_options(stream_results=True)
    chunks = []
    
    try:
        print(f"Executing query...")
        result = connection.execute(text(query), params or {})
        
        total_rows = 0
        chunk_num = 0
        
        while True:
            try:
                chunk = result.fetchmany(chunk_size)
                if not chunk:
                    break
                    
                chunk_df = pd.DataFrame(chunk, columns=result.keys())
                chunks.append(chunk_df)
                total_rows += len(chunk_df)
                chunk_num += 1
                
                elapsed = time.time() - start_time
                rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
                
                print(f"Fetched {total_rows} rows so far... ({rows_per_sec:.2f} rows/sec, chunk {chunk_num})")
                
                # Write chunk to disk to avoid memory issues
                if len(chunks) >= 5:  # After accumulating 5 chunks
                    print(f"Writing chunks to {filename}...")
                    combined = pd.concat(chunks, ignore_index=True)
                    if not os.path.exists(filename):
                        combined.to_csv(filename, index=False)
                    else:
                        combined.to_csv(filename, mode='a', header=False, index=False)
                    chunks = []  # Clear the chunks from memory
                    print(f"Wrote {len(combined)} rows to file, continuing fetch...")
            except Exception as e:
                print(f"Error processing chunk {chunk_num}: {str(e)}")
                continue
                
        # Process any remaining chunks
        if chunks:
            print(f"Writing final chunks to {filename}...")
            combined = pd.concat(chunks, ignore_index=True)
            if not os.path.exists(filename):
                combined.to_csv(filename, index=False)
            else:
                combined.to_csv(filename, mode='a', header=False, index=False)
                
        total_time = time.time() - start_time
        print(f"Fetch completed in {total_time:.2f} seconds")
        return total_rows
        
    except Exception as e:
        print(f"Error in fetch_in_chunks: {str(e)}")
        return 0
    finally:
        connection.close()

# Fetch and save darkpool trades in chunks
print("\nFetching and saving darkpool trades...")
params = {'seven_days_ago': seven_days_ago}
total_darkpool_rows = fetch_in_chunks(darkpool_query, engine, darkpool_filename, params)
print(f"Completed saving {total_darkpool_rows} darkpool trades to {darkpool_filename}")

# Fetch and save options flow data in chunks
print("\nFetching and saving options flow data...")
total_options_rows = fetch_in_chunks(options_query, engine, options_filename, params)
print(f"Completed saving {total_options_rows} option flows to {options_filename}")

print("\nFull data fetch complete.")
print("\nGenerating summary statistics...")

# Process and summarize data from the saved files
try:
    print("\nGenerating summary statistics...")
    
    # Process darkpool trades if file exists
    if os.path.exists(darkpool_filename):
        trades_sample = pd.read_csv(darkpool_filename, nrows=100000)
        
        # Process darkpool trades
        trades_sample['executed_at'] = pd.to_datetime(trades_sample['executed_at'])
        if 'collection_time' in trades_sample.columns:
            trades_sample['collection_time'] = pd.to_datetime(trades_sample['collection_time'])
        trades_sample['trade_hour'] = pd.to_datetime(trades_sample['trade_hour'])
        
        # Print darkpool trade summary
        print("\nDarkpool Trade summary by symbol (last 7 days):")
        summary = trades_sample.groupby('symbol').agg({
            'size': ['count', 'sum', 'mean'],
            'premium': ['mean', 'max'],
            'price_impact_pct': 'mean'
        }).round(2)
        print(summary)
        
        # Print date ranges
        print("\nDate ranges (last 7 days):")
        print("Darkpool Trades:")
        print(f"Earliest trade in sample: {trades_sample['executed_at'].min()}")
        print(f"Latest trade in sample: {trades_sample['executed_at'].max()}")
        print(f"Total trades fetched: {total_darkpool_rows}")
        
        # Print daily distribution
        print("\nDaily Trade Distribution:")
        trades_sample['date'] = trades_sample['executed_at'].dt.date
        daily_stats = trades_sample.groupby(['date', 'symbol']).agg({
            'size': ['count', 'sum', 'mean'],
            'premium': ['sum', 'mean']
        }).round(2)
        print(daily_stats)
    else:
        print("\nNo darkpool trades found in the last 7 days.")
    
    # Process options flow if file exists
    if os.path.exists(options_filename) and total_options_rows > 0:
        options_sample = pd.read_csv(options_filename, nrows=100000)
        
        # Process options flow
        options_sample['collected_at'] = pd.to_datetime(options_sample['collected_at'])
        if 'created_at' in options_sample.columns:
            options_sample['created_at'] = pd.to_datetime(options_sample['created_at'])
        if 'expiry' in options_sample.columns:
            options_sample['expiry'] = pd.to_datetime(options_sample['expiry'])
        options_sample['flow_hour'] = pd.to_datetime(options_sample['flow_hour'])
        
        # Print options flow summary
        print("\nOptions Flow summary by symbol (last 7 days):")
        print(options_sample.groupby('symbol').agg({
            'premium': ['count', 'sum', 'mean', 'max'],
            'contract_size': ['sum', 'mean'],
            'iv_rank': 'mean'
        }).round(2))
        
        print("\nOptions Flow:")
        print(f"Earliest flow in sample: {options_sample['collected_at'].min()}")
        print(f"Latest flow in sample: {options_sample['collected_at'].max()}")
        print(f"Total flows fetched: {total_options_rows}")
    else:
        print("\nNo options flow data found in the last 7 days.")
    
except Exception as e:
    print(f"Error generating summary statistics: {str(e)}")
    print("Data has been saved to files, but summary statistics could not be generated.")

print("\nFull data is available in:")
if total_darkpool_rows > 0:
    print(f"- {darkpool_filename}")
if total_options_rows > 0:
    print(f"- {options_filename}") 