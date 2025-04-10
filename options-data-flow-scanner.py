"""
flow_scanner.py
This script analyzes options flow data to identify unusual institutional activity.

Usage:
    python flow_scanner.py --date 2025-04-05
    python flow_scanner.py  # Uses today's date by default
"""

import os
import argparse
import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/flow_scanner.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import configuration
import sys
sys.path.append('../')
from config.watchlist import WATCHLIST
from config.thresholds import PREMIUM_THRESHOLD, ZSCORE_THRESHOLD

class InstitutionalFlowScanner:
    """Analyzes options flow data to identify unusual institutional activity"""
    
    def __init__(self):
        # Create directories if they don't exist
        Path("../data/processed").mkdir(parents=True, exist_ok=True)
        
    def _load_historical_data(self, ticker, days=30):
        """Load historical options flow data for a ticker"""
        today = datetime.datetime.now().date()
        
        # Load data from the last N days
        all_data = []
        for i in range(1, days + 1):
            date = today - datetime.timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")
            file_path = f"../data/processed/{date_str}/institutional_flow.csv"
            
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    ticker_data = df[df['ticker'] == ticker]
                    all_data.append(ticker_data)
                except Exception as e:
                    logger.warning(f"Error loading data for {ticker} on {date_str}: {e}")
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        else:
            return pd.DataFrame()
    
    def _calculate_zscore(self, df, ticker, contract_type, strike, expiration):
        """Calculate Z-score for a specific options contract based on historical premium"""
        historical_df = self._load_historical_data(ticker)
        
        if historical_df.empty:
            return 0
        
        # Filter historical data for this specific contract
        filtered_df = historical_df[
            (historical_df['ticker'] == ticker) &
            (historical_df['contract_type'] == contract_type) &
            (historical_df['strike'] == strike) &
            (historical_df['expiration'] == expiration)
        ]
        
        if filtered_df.empty or len(filtered_df) < 3:
            # Not enough historical data, use the broader contract type
            filtered_df = historical_df[
                (historical_df['ticker'] == ticker) &
                (historical_df['contract_type'] == contract_type)
            ]
        
        if filtered_df.empty or len(filtered_df) < 3:
            return 0
        
        # Calculate Z-score based on premium
        mean_premium = filtered_df['premium'].mean()
        std_premium = filtered_df['premium'].std()
        
        if std_premium == 0:
            return 0
        
        current_premium = df['premium'].iloc[0]
        zscore = (current_premium - mean_premium) / std_premium
        
        return zscore
    
    def process_options_trades(self, date=None):
        """Process options trades to identify unusual institutional flow"""
        if date is None:
            date = datetime.datetime.now().date()
        
        date_str = date.strftime("%Y-%m-%d")
        logger.info(f"Processing options trades for {date_str}")
        
        # Load raw options trades data
        trades_file = f"../data/raw/{date_str}/options_trades.csv"
        if not os.path.exists(trades_file):
            logger.error(f"Options trades file not found: {trades_file}")
            return None
        
        df_trades = pd.read_csv(trades_file)
        
        # Prepare data for analysis
        if 'size' in df_trades.columns and 'price' in df_trades.columns:
            df_trades['premium'] = df_trades['size'] * df_trades['price'] * 100  # Convert to total premium value
        
        # Extract contract details
        if 'symbol' in df_trades.columns:
            # Parse contract symbol (e.g., AAPL250417C00200000)
            df_trades['ticker'] = df_trades['symbol'].str.extract(r'^([A-Z]+)')
            df_trades['expiration'] = df_trades['symbol'].str.extract(r'([0-9]{6})[CP]')
            df_trades['contract_type'] = df_trades['symbol'].str.extract(r'[0-9]{6}([CP])')
            df_trades['strike'] = df_trades['symbol'].str.extract(r'[0-9]{6}[CP]([0-9]+)')
            
            # Convert strike to float (handle decimal point)
            df_trades['strike'] = df_trades['strike'].astype(float) / 1000
            
            # Convert expiration to date format
            df_trades['expiration'] = pd.to_datetime(df_trades['expiration'], format='%y%m%d')
        
        # Filter for high premium trades (potential institutional activity)
        institutional_flow = df_trades[df_trades['premium'] >= PREMIUM_THRESHOLD].copy()
        
        if institutional_flow.empty:
            logger.info("No institutional flow detected")
            return None
        
        # Calculate Z-scores for each trade
        institutional_flow['z_score'] = 0
        for idx, row in institutional_flow.iterrows():
            ticker = row['ticker']
            contract_type = row['contract_type']
            strike = row['strike']
            expiration = row['expiration']
            
            z_score = self._calculate_zscore(
                institutional_flow.iloc[[idx]], 
                ticker, 
                contract_type, 
                strike, 
                expiration
            )
            institutional_flow.loc[idx, 'z_score'] = z_score
        
        # Flag unusual trades
        institutional_flow['unusual_flag'] = institutional_flow['z_score'].abs() >= ZSCORE_THRESHOLD
        
        # Save processed data
        output_dir = f"../data/processed/{date_str}"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        institutional_flow.to_csv(f"{output_dir}/institutional_flow.csv", index=False)
        
        # Generate summary
        summary = institutional_flow.groupby(['ticker', 'contract_type']).agg({
            'premium': 'sum',
            'unusual_flag': 'sum'
        }).reset_index()
        summary.to_csv(f"{output_dir}/institutional_flow_summary.csv", index=False)
        
        logger.info(f"Saved institutional flow data with {len(institutional_flow)} records")
        logger.info(f"Detected {institutional_flow['unusual_flag'].sum()} unusual trades")
        
        # Return unusual trades
        return institutional_flow[institutional_flow['unusual_flag']]
    
    def visualize_institutional_flow(self, date=None):
        """Create visualizations of institutional flow"""
        if date is None:
            date = datetime.datetime.now().date()
        
        date_str = date.strftime("%Y-%m-%d")
        
        # Load processed data
        file_path = f"../data/processed/{date_str}/institutional_flow.csv"
        if not os.path.exists(file_path):
            logger.error(f"Institutional flow file not found: {file_path}")
            return
        
        df = pd.read_csv(file_path)
        
        if df.empty:
            logger.info("No institutional flow data to visualize")
            return
        
        # Create output directory for visualizations
        output_dir = f"../data/processed/{date_str}/visualizations"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 1. Premium by ticker and contract type
        plt.figure(figsize=(14, 8))
        
        # Group by ticker and contract type
        grouped = df.groupby(['ticker', 'contract_type'])['premium'].sum().unstack()
        
        # Normalize to millions for better visualization
        grouped = grouped / 1_000_000
        
        # Create bar chart
        ax = grouped.plot(kind='bar', stacked=True, figsize=(14, 8))
        plt.title(f'Institutional Options Flow by Ticker ({date_str})', fontsize=16)
        plt.xlabel('Ticker', fontsize=14)
        plt.ylabel('Premium ($ millions)', fontsize=14)
        plt.xticks(rotation=45)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.legend(title='Contract Type')
        
        # Save visualization
        plt.tight_layout()
        plt.savefig(f"{output_dir}/institutional_flow_by_ticker.png")
        plt.close()
        
        # 2. Unusual trades visualization
        unusual_trades = df[df['unusual_flag']]
        
        if not unusual_trades.empty:
            plt.figure(figsize=(14, 8))
            
            # Create scatter plot of unusual trades
            plt.scatter(
                unusual_trades['z_score'],
                unusual_trades['premium'] / 1_000_000,
                c=unusual_trades['contract_type'].map({'C': 'green', 'P': 'red'}),
                alpha=0.7,
                s=100
            )
            
            plt.title(f'Unusual Options Flow ({date_str})', fontsize=16)
            plt.xlabel('Z-Score (Unusualness)', fontsize=14)
            plt.ylabel('Premium ($ millions)', fontsize=14)
            plt.grid(True, linestyle='--', alpha=0.7)
            
            # Add ticker annotations
            for _, row in unusual_trades.iterrows():
                plt.annotate(
                    f"{row['ticker']} {row['strike']} {row['contract_type']}",
                    (row['z_score'], row['premium'] / 1_000_000),
                    xytext=(5, 5),
                    textcoords='offset points'
                )
            
            plt.tight_layout()
            plt.savefig(f"{output_dir}/unusual_trades.png")
            plt.close()
        
        logger.info(f"Created institutional flow visualizations in {output_dir}")


def main():
    """Main function to run the script"""
    parser = argparse.ArgumentParser(description="Analyze options flow data")
    parser.add_argument("--date", help="Date to analyze data for (YYYY-MM-DD)")
    args = parser.parse_args()
    
    if args.date:
        date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    else:
        date = datetime.datetime.now().date()
    
    scanner = InstitutionalFlowScanner()
    unusual_trades = scanner.process_options_trades(date)
    
    if unusual_trades is not None and not unusual_trades.empty:
        scanner.visualize_institutional_flow(date)
        print(f"Detected {len(unusual_trades)} unusual trades")
        print(unusual_trades[['ticker', 'contract_type', 'strike', 'expiration', 'premium', 'z_score']])


if __name__ == "__main__":
    # Create logs directory if it doesn't exist
    Path("logs").mkdir(exist_ok=True)
    main()
