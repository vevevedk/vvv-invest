"""
Dark Pool Flow Scanner
Analyzes dark pool trade data to identify significant trading activity
"""

import os
import sys
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from flow_analysis.config.watchlist import (
    SYMBOLS, BLOCK_SIZE_THRESHOLD, PREMIUM_THRESHOLD,
    PRICE_IMPACT_THRESHOLD, MARKET_OPEN, MARKET_CLOSE,
    INTRADAY_WINDOW, HISTORICAL_WINDOW, REALTIME_WINDOW
)
from scripts.data_fetcher import DarkPoolDataFetcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DarkPoolFlowScanner:
    def __init__(self):
        self.fetcher = DarkPoolDataFetcher()
        self.processed_data_dir = project_root / "data/processed"
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Convert time windows to pandas frequency strings
        self.realtime_window = "5min"  # Fixed frequency for real-time analysis
        self.intraday_window = "1H"    # Fixed frequency for intraday analysis
        self.historical_window = "5D"  # Fixed frequency for historical analysis

    def analyze_trades(self, trades: pd.DataFrame) -> pd.DataFrame:
        """Analyze trades to identify significant activity"""
        if trades.empty:
            logger.warning("No trades to analyze")
            return pd.DataFrame()

        # Ensure numeric columns are properly typed
        numeric_columns = ["price", "size", "premium", "nbbo_ask", "nbbo_bid", "price_impact"]
        for col in numeric_columns:
            if col in trades.columns:
                trades[col] = pd.to_numeric(trades[col], errors='coerce')

        # Add time-based analysis
        trades["hour"] = trades["timestamp"].dt.hour
        trades["minute"] = trades["timestamp"].dt.minute
        trades["time_bucket"] = trades["timestamp"].dt.floor(self.realtime_window)

        # Calculate additional metrics
        trades["volume_weighted_price"] = trades["price"] * trades["size"]
        trades["price_impact_score"] = trades["price_impact"] * trades["size"]
        trades["relative_size"] = trades["size"] / trades["size"].mean()
        trades["relative_premium"] = trades["premium"] / trades["premium"].mean()
        trades["trade_score"] = (trades["relative_size"] * 0.4 + 
                               trades["relative_premium"] * 0.3 + 
                               trades["price_impact"] * 0.3)

        # Group by time bucket for analysis
        grouped = trades.groupby(["ticker", "time_bucket"]).agg({
            "size": ["sum", "count", "mean", "max", "std"],
            "premium": ["sum", "mean", "max", "std"],
            "price": ["mean", "std"],
            "price_impact": ["mean", "max", "std"],
            "volume_weighted_price": "sum",
            "price_impact_score": "sum",
            "is_block_trade": "sum",
            "is_high_premium": "sum",
            "is_price_impact": "sum",
            "trade_score": ["mean", "max"]
        }).reset_index()

        # Flatten column names
        grouped.columns = ["_".join(col).strip("_") for col in grouped.columns.values]

        # Calculate derived metrics
        grouped["vwap"] = grouped["volume_weighted_price_sum"] / grouped["size_sum"]
        grouped["avg_trade_size"] = grouped["size_sum"] / grouped["size_count"]
        grouped["block_trade_ratio"] = grouped["is_block_trade_sum"] / grouped["size_count"]
        grouped["high_premium_ratio"] = grouped["is_high_premium_sum"] / grouped["size_count"]
        grouped["price_impact_ratio"] = grouped["is_price_impact_sum"] / grouped["size_count"]
        
        # Add volatility metrics
        grouped["size_volatility"] = grouped["size_std"] / grouped["size_mean"]
        grouped["price_volatility"] = grouped["price_std"] / grouped["price_mean"]
        grouped["premium_volatility"] = grouped["premium_std"] / grouped["premium_mean"]
        
        # Add concentration metrics
        grouped["volume_concentration"] = grouped["size_max"] / grouped["size_sum"]
        grouped["premium_concentration"] = grouped["premium_max"] / grouped["premium_sum"]

        # Replace any infinite values with NaN
        grouped = grouped.replace([np.inf, -np.inf], np.nan)

        return grouped

    def generate_alerts(self, analysis: pd.DataFrame) -> pd.DataFrame:
        """Generate alerts for unusual trading activity"""
        alerts = []

        for _, row in analysis.iterrows():
            alert = {
                "ticker": row["ticker"],
                "timestamp": row["time_bucket"],
                "alert_type": [],
                "details": {}
            }

            # Check for unusual volume (reduced threshold)
            if row["size_sum"] > BLOCK_SIZE_THRESHOLD * 2:  # Reduced from 5x to 2x
                alert["alert_type"].append("HIGH_VOLUME")
                alert["details"]["volume"] = row["size_sum"]
                alert["details"]["threshold"] = BLOCK_SIZE_THRESHOLD * 2

            # Check for high premium concentration (reduced threshold)
            if row["high_premium_ratio"] > 0.3:  # Reduced from 0.5 to 0.3
                alert["alert_type"].append("HIGH_PREMIUM_CONCENTRATION")
                alert["details"]["premium_ratio"] = row["high_premium_ratio"]
                alert["details"]["threshold"] = 0.3

            # Check for significant price impact (reduced threshold)
            if row["price_impact_ratio"] > 0.15:  # Reduced from 0.3 to 0.15
                alert["alert_type"].append("HIGH_PRICE_IMPACT")
                alert["details"]["impact_ratio"] = row["price_impact_ratio"]
                alert["details"]["threshold"] = 0.15

            # Check for unusual block trade concentration (reduced threshold)
            if row["block_trade_ratio"] > 0.2:  # Reduced from 0.4 to 0.2
                alert["alert_type"].append("HIGH_BLOCK_TRADE_CONCENTRATION")
                alert["details"]["block_ratio"] = row["block_trade_ratio"]
                alert["details"]["threshold"] = 0.2

            # Check for high volatility
            if row["size_volatility"] > 2.0:  # More than 2x average volatility
                alert["alert_type"].append("HIGH_VOLUME_VOLATILITY")
                alert["details"]["volatility"] = row["size_volatility"]
                alert["details"]["threshold"] = 2.0

            # Check for high concentration
            if row["volume_concentration"] > 0.5:  # More than 50% volume in single trade
                alert["alert_type"].append("HIGH_VOLUME_CONCENTRATION")
                alert["details"]["concentration"] = row["volume_concentration"]
                alert["details"]["threshold"] = 0.5

            # Check for high trade score
            if row["trade_score_max"] > 2.0:  # More than 2x average trade score
                alert["alert_type"].append("HIGH_TRADE_SCORE")
                alert["details"]["score"] = row["trade_score_max"]
                alert["details"]["threshold"] = 2.0

            if alert["alert_type"]:
                alerts.append(alert)

        return pd.DataFrame(alerts)

    def visualize_flow(self, analysis: pd.DataFrame, output_dir: Path = None):
        """Create visualizations of dark pool flow"""
        if output_dir is None:
            output_dir = self.processed_data_dir / "visualizations"
        output_dir.mkdir(parents=True, exist_ok=True)

        for symbol in SYMBOLS:
            symbol_data = analysis[analysis["ticker"] == symbol]
            if symbol_data.empty:
                continue

            # Create time series plots
            plt.figure(figsize=(15, 10))
            
            # Volume plot
            plt.subplot(3, 1, 1)
            plt.plot(symbol_data["time_bucket"], symbol_data["size_sum"], label="Volume")
            plt.title(f"{symbol} Dark Pool Volume")
            plt.xticks(rotation=45)
            plt.legend()

            # Premium plot
            plt.subplot(3, 1, 2)
            plt.plot(symbol_data["time_bucket"], symbol_data["premium_sum"], label="Premium")
            plt.title(f"{symbol} Dark Pool Premium")
            plt.xticks(rotation=45)
            plt.legend()

            # Price impact plot
            plt.subplot(3, 1, 3)
            plt.plot(symbol_data["time_bucket"], symbol_data["price_impact_mean"], label="Price Impact")
            plt.title(f"{symbol} Dark Pool Price Impact")
            plt.xticks(rotation=45)
            plt.legend()

            plt.tight_layout()
            plt.savefig(output_dir / f"{symbol}_flow_{datetime.now().strftime('%Y%m%d')}.png")
            plt.close()

    def save_analysis(self, analysis: pd.DataFrame, alerts: pd.DataFrame):
        """Save analysis results and alerts"""
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Save analysis
        analysis_file = self.processed_data_dir / f"flow_analysis_{date_str}.csv"
        analysis.to_csv(analysis_file, index=False)
        logger.info(f"Saved analysis to {analysis_file}")

        # Save alerts
        if not alerts.empty:
            alerts_file = self.processed_data_dir / f"flow_alerts_{date_str}.csv"
            alerts.to_csv(alerts_file, index=False)
            logger.info(f"Saved alerts to {alerts_file}")

    def run_analysis(self):
        """Run complete flow analysis"""
        # Fetch recent trades
        logger.info("Fetching recent dark pool trades...")
        trades = self.fetcher.fetch_recent_trades()
        
        if trades.empty:
            logger.warning("No trades to analyze")
            return
            
        # Analyze trades
        logger.info("Analyzing trades...")
        analysis = self.analyze_trades(trades)
        
        # Generate alerts
        logger.info("Generating alerts...")
        alerts = self.generate_alerts(analysis)
        
        # Create visualizations
        logger.info("Creating visualizations...")
        self.visualize_flow(analysis)
        
        # Save results
        logger.info("Saving results...")
        self.save_analysis(analysis, alerts)
        
        # Log summary
        logger.info("\nAnalysis Summary:")
        logger.info(f"Total Trades Analyzed: {len(trades)}")
        logger.info(f"Time Periods Analyzed: {len(analysis)}")
        logger.info(f"Alerts Generated: {len(alerts)}")
        
        for symbol in SYMBOLS:
            symbol_data = analysis[analysis["ticker"] == symbol]
            if not symbol_data.empty:
                logger.info(f"\n{symbol} Summary:")
                logger.info(f"Total Volume: {symbol_data['size_sum'].sum():,.0f}")
                logger.info(f"Total Premium: ${symbol_data['premium_sum'].sum():,.2f}")
                logger.info(f"Average Price Impact: {symbol_data['price_impact_mean'].mean():.2%}")

def main():
    scanner = DarkPoolFlowScanner()
    scanner.run_analysis()

if __name__ == "__main__":
    main() 