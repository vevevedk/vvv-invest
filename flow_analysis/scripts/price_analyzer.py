"""
Price Level Analysis
Analyzes dark pool trades by price level and correlates with options strike prices
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
    PRICE_IMPACT_THRESHOLD, MARKET_OPEN, MARKET_CLOSE
)
from scripts.data_fetcher import DarkPoolDataFetcher
from scripts.options_fetcher import OptionsDataFetcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PriceLevelAnalyzer:
    def __init__(self):
        self.fetcher = DarkPoolDataFetcher()
        self.options_fetcher = OptionsDataFetcher()
        self.processed_data_dir = project_root / "data/processed"
        self.processed_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Price level configuration
        self.price_level_size = 0.25  # Reduced from 0.5 for finer granularity
        self.min_strike_gap = 0.5     # Reduced from 1.0 for better strike correlation
        self.volume_threshold = BLOCK_SIZE_THRESHOLD * 0.5  # 50% of block size
        self.concentration_threshold = 0.4  # 40% concentration threshold
        self.impact_threshold = 0.15  # 15% price impact threshold

    def calculate_price_levels(self, trades: pd.DataFrame) -> pd.DataFrame:
        """Calculate price levels and aggregate trades"""
        if trades.empty:
            logger.warning("No trades to analyze")
            return pd.DataFrame()

        # Ensure numeric columns are properly typed
        numeric_columns = ["price", "size", "premium", "nbbo_ask", "nbbo_bid", "price_impact"]
        for col in numeric_columns:
            if col in trades.columns:
                trades[col] = pd.to_numeric(trades[col], errors='coerce')

        # Calculate price levels with finer granularity
        trades["price_level"] = (trades["price"] / self.price_level_size).round() * self.price_level_size
        
        # Group by price level with enhanced metrics
        grouped = trades.groupby(["ticker", "price_level"]).agg({
            "size": ["sum", "count", "mean", "max", "std"],
            "premium": ["sum", "mean", "max", "std"],
            "price_impact": ["mean", "max", "std"],
            "is_block_trade": "sum",
            "is_high_premium": "sum",
            "is_price_impact": "sum"
        }).reset_index()

        # Flatten column names
        grouped.columns = ["_".join(col).strip("_") for col in grouped.columns.values]

        # Calculate derived metrics
        grouped["volume_concentration"] = grouped["size_max"] / grouped["size_sum"]
        grouped["premium_concentration"] = grouped["premium_max"] / grouped["premium_sum"]
        grouped["block_trade_ratio"] = grouped["is_block_trade_sum"] / grouped["size_count"]
        grouped["high_premium_ratio"] = grouped["is_high_premium_sum"] / grouped["size_count"]
        grouped["price_impact_ratio"] = grouped["is_price_impact_sum"] / grouped["size_count"]
        
        # Add volatility metrics
        grouped["volume_volatility"] = grouped["size_std"] / grouped["size_mean"]
        grouped["premium_volatility"] = grouped["premium_std"] / grouped["premium_mean"]
        grouped["impact_volatility"] = grouped["price_impact_std"] / grouped["price_impact_mean"]

        return grouped

    def correlate_with_strikes(self, price_levels: pd.DataFrame) -> pd.DataFrame:
        """Correlate price levels with options strike prices"""
        if price_levels.empty:
            logger.warning("No price levels to correlate")
            return pd.DataFrame()

        # Fetch strike prices for all symbols
        strikes = self.options_fetcher.fetch_all_strikes()
        if strikes.empty:
            logger.warning("No strike prices available")
            return price_levels

        # Find nearest strike for each price level
        price_levels["nearest_strike"] = price_levels.apply(
            lambda row: min(
                strikes[strikes["symbol"] == row["ticker"]]["strike"],
                key=lambda x: abs(x - row["price_level"])
            ),
            axis=1
        )
        
        # Calculate distance to nearest strike
        price_levels["strike_distance"] = abs(price_levels["price_level"] - price_levels["nearest_strike"])
        
        # Flag price levels near strikes
        price_levels["near_strike"] = price_levels["strike_distance"] <= self.min_strike_gap
        
        return price_levels

    def create_heat_map(self, analysis: pd.DataFrame, output_dir: Path = None):
        """Create enhanced heat map visualization of price level activity"""
        if output_dir is None:
            output_dir = self.processed_data_dir / "visualizations"
        output_dir.mkdir(parents=True, exist_ok=True)

        for symbol in SYMBOLS:
            symbol_data = analysis[analysis["ticker"] == symbol]
            if symbol_data.empty:
                continue

            # Create multiple heat maps
            metrics = {
                "volume": "size_sum",
                "concentration": "volume_concentration",
                "impact": "price_impact_mean"
            }

            for metric_name, metric_col in metrics.items():
                # Create heat map data
                heat_data = symbol_data.pivot_table(
                    values=metric_col,
                    index="price_level",
                    columns="near_strike",
                    aggfunc="mean",
                    fill_value=0
                )

                # Create the heat map
                plt.figure(figsize=(12, 8))
                sns.heatmap(
                    heat_data,
                    cmap="YlOrRd",
                    annot=True,
                    fmt=".2f",
                    cbar_kws={"label": metric_name.title()}
                )
                plt.title(f"{symbol} Dark Pool {metric_name.title()} by Price Level")
                plt.xlabel("Near Strike Price")
                plt.ylabel("Price Level")
                
                plt.tight_layout()
                plt.savefig(output_dir / f"{symbol}_{metric_name}_heatmap_{datetime.now().strftime('%Y%m%d')}.png")
                plt.close()

    def generate_alerts(self, analysis: pd.DataFrame) -> pd.DataFrame:
        """Generate enhanced alerts for unusual price level activity"""
        alerts = []

        for _, row in analysis.iterrows():
            alert = {
                "ticker": row["ticker"],
                "price_level": row["price_level"],
                "alert_type": [],
                "details": {}
            }

            # Check for high volume concentration
            if row["volume_concentration"] > self.concentration_threshold:
                alert["alert_type"].append("HIGH_VOLUME_CONCENTRATION")
                alert["details"]["concentration"] = row["volume_concentration"]

            # Check for high premium concentration
            if row["premium_concentration"] > self.concentration_threshold:
                alert["alert_type"].append("HIGH_PREMIUM_CONCENTRATION")
                alert["details"]["concentration"] = row["premium_concentration"]

            # Check for unusual block trade concentration
            if row["block_trade_ratio"] > 0.3:
                alert["alert_type"].append("HIGH_BLOCK_TRADE_CONCENTRATION")
                alert["details"]["ratio"] = row["block_trade_ratio"]

            # Check for high price impact
            if row["price_impact_ratio"] > self.impact_threshold:
                alert["alert_type"].append("HIGH_PRICE_IMPACT")
                alert["details"]["ratio"] = row["price_impact_ratio"]

            # Check for unusual activity near strikes
            if row["near_strike"] and row["size_sum"] > self.volume_threshold:
                alert["alert_type"].append("HIGH_VOLUME_NEAR_STRIKE")
                alert["details"]["volume"] = row["size_sum"]
                alert["details"]["strike_distance"] = row["strike_distance"]

            # Check for high volatility
            if row["volume_volatility"] > 1.0:
                alert["alert_type"].append("HIGH_VOLUME_VOLATILITY")
                alert["details"]["volatility"] = row["volume_volatility"]

            if alert["alert_type"]:
                alerts.append(alert)

        return pd.DataFrame(alerts)

    def save_analysis(self, analysis: pd.DataFrame, alerts: pd.DataFrame):
        """Save analysis results and alerts"""
        date_str = datetime.now().strftime("%Y%m%d")
        
        # Save analysis
        analysis_file = self.processed_data_dir / f"price_levels_{date_str}.csv"
        analysis.to_csv(analysis_file, index=False)
        logger.info(f"Saved analysis to {analysis_file}")

        # Save alerts
        if not alerts.empty:
            alerts_file = self.processed_data_dir / f"price_alerts_{date_str}.csv"
            alerts.to_csv(alerts_file, index=False)
            logger.info(f"Saved alerts to {alerts_file}")

    def run_analysis(self):
        """Run complete price level analysis"""
        # Fetch recent trades
        logger.info("Fetching recent dark pool trades...")
        trades = self.fetcher.fetch_recent_trades()
        
        if trades.empty:
            logger.warning("No trades to analyze")
            return
            
        # Calculate price levels
        logger.info("Calculating price levels...")
        price_levels = self.calculate_price_levels(trades)
        
        # Correlate with strikes
        logger.info("Correlating with strike prices...")
        analysis = self.correlate_with_strikes(price_levels)
        
        # Generate alerts
        logger.info("Generating alerts...")
        alerts = self.generate_alerts(analysis)
        
        # Create visualizations
        logger.info("Creating visualizations...")
        self.create_heat_map(analysis)
        
        # Save results
        logger.info("Saving results...")
        self.save_analysis(analysis, alerts)
        
        # Log summary
        logger.info("\nAnalysis Summary:")
        logger.info(f"Total Price Levels: {len(analysis)}")
        logger.info(f"Alerts Generated: {len(alerts)}")
        
        for symbol in SYMBOLS:
            symbol_data = analysis[analysis["ticker"] == symbol]
            if not symbol_data.empty:
                logger.info(f"\n{symbol} Summary:")
                logger.info(f"Price Levels: {len(symbol_data)}")
                logger.info(f"Near Strike Levels: {symbol_data['near_strike'].sum()}")
                logger.info(f"Total Volume: {symbol_data['size_sum'].sum():,.0f}")
                logger.info(f"Total Premium: ${symbol_data['premium_sum'].sum():,.2f}")
                logger.info(f"Average Price Impact: {symbol_data['price_impact_mean'].mean():.2%}")

def main():
    analyzer = PriceLevelAnalyzer()
    analyzer.run_analysis()

if __name__ == "__main__":
    main() 