"""
Watchlist Configuration
"""

# Target Symbols
SYMBOLS = [
    # Core Market ETFs
    "SPY",  # S&P 500 ETF
    "QQQ",  # Nasdaq 100 ETF
    
    # Safe Haven Assets
    "GLD",  # Gold ETF
    "SLV",  # Silver ETF
    "TLT",  # 20+ Year Treasury ETF
    "FXF",  # Swiss Franc Trust
    
    # Sector ETFs
    "XLF",  # Financial Sector ETF
    "SMH",  # Semiconductor ETF
    "XLE",  # Energy Sector ETF
    
    # International ETFs
    "EZU",  # Eurozone ETF
    "EFA"   # Developed Markets ex-US
]

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