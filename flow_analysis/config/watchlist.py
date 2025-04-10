"""
Watchlist Configuration
"""

# Target Symbols
SYMBOLS = ["SPY", "QQQ"]

# Dark Pool Trade Thresholds
BLOCK_SIZE_THRESHOLD = 10000  # minimum shares for block trade
PREMIUM_THRESHOLD = 1000000   # minimum dollar value for significant trade
PRICE_IMPACT_THRESHOLD = 0.1  # % move from NBBO midpoint

# Time Windows for Analysis
INTRADAY_WINDOW = "1H"       # for intraday volume analysis
HISTORICAL_WINDOW = "5D"      # for historical comparison
REALTIME_WINDOW = "5M"       # for real-time monitoring

# Market Hours (Eastern Time)
MARKET_OPEN = "09:30"
MARKET_CLOSE = "16:00"
EXTENDED_HOURS_START = "04:00"
EXTENDED_HOURS_END = "20:00" 