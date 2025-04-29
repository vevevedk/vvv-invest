"""
Watchlist and market configuration
"""

from datetime import time

# Symbols to track
SYMBOLS = ["SPY", "QQQ", "GLD"]

# Trade thresholds
BLOCK_SIZE_THRESHOLD = 10000  # Minimum size for a block trade
PREMIUM_THRESHOLD = 1000000  # Minimum premium for a significant trade
PRICE_IMPACT_THRESHOLD = 0.1  # Minimum price impact percentage

# Market hours (Eastern Time)
MARKET_OPEN = time(9, 30)  # 9:30 AM
MARKET_CLOSE = time(16, 0)  # 4:00 PM 