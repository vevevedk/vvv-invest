"""
strike_analyzer.py
This script analyzes options data to identify strike price concentrations,
highlighting where institutional investors are positioning.

Usage:
    python strike_analyzer.py --date 2025-04-05 --ticker SPY
    python strike_analyzer.py  # Uses today's date and all tickers in watchlist
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
        logging.FileHandler("logs/strike_analyzer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import configuration
import sys
sys.path.append('../')
from config.watchlist import WATCHLIST
from config.thresholds import CONCENTRATION_THRESHOLD

class StrikeConcentrationAnalyzer:
    """Analyzes options data to identify strike price concentrations"""
    
    def __init__(self):
        # Create directories if they don't exist
        Path("../data/processed").mkdir(parents=True, exist_ok=True)
    
    def analyze_strike_concentration(self, date=None, ticker=None):
        """Analyze strike price concentration for a given date and ticker"""
        if date is None:
            date = datetime.datetime.now().date()
        
        date_str = date.strftime("%Y-%m-%d")
        logger.info(f"Analyzing strike concentration for {date_str}")
        
        # Load raw options trades data
        trades_file = f"../data/raw/{date_str}/options_trades.csv"
        if not os.path.exists(trades_file):
            logger.error(f"Options trades file not found: {trades_file}")
            return None
        
        df_trades = pd.read_csv(trades_file)
        
        # Filter by ticker if specified
        if ticker:
            if 'ticker' in df_trades.columns: