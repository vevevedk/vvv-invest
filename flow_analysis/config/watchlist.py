"""
Watchlist Configuration
"""

from datetime import datetime, time
import pytz

# Market Hours (Eastern Time)
MARKET_OPEN = time(9, 30)  # 9:30 AM ET
MARKET_CLOSE = time(16, 0)  # 4:00 PM ET

# Market Holidays for 2024
MARKET_HOLIDAYS = [
    datetime(2024, 1, 1),   # New Year's Day
    datetime(2024, 1, 15),  # Martin Luther King Jr. Day
    datetime(2024, 2, 19),  # Presidents' Day
    datetime(2024, 3, 29),  # Good Friday
    datetime(2024, 5, 27),  # Memorial Day
    datetime(2024, 6, 19),  # Juneteenth
    datetime(2024, 7, 4),   # Independence Day
    datetime(2024, 9, 2),   # Labor Day
    datetime(2024, 11, 28), # Thanksgiving Day
    datetime(2024, 12, 25)  # Christmas Day
]

# Target Symbols
SYMBOLS = [
    # Core Market ETFs
    "SPY",  # S&P 500 ETF
    "QQQ",  # Nasdaq 100 ETF
    "IWM",  # Russell 2000 ETF
    "DIA",  # Dow Jones ETF
    
    # Volatility
    "VIX",  # Volatility Index
]

# Analysis Thresholds
BLOCK_SIZE_THRESHOLD = 10000  # minimum shares for block trade
PREMIUM_THRESHOLD = 1000000   # minimum dollar value for significant trade
PRICE_IMPACT_THRESHOLD = 0.1  # % move from NBBO midpoint

# Time Windows for Analysis
INTRADAY_WINDOW = "1H"       # for intraday volume analysis
HISTORICAL_WINDOW = "5D"      # for historical comparison
REALTIME_WINDOW = "5M"       # for real-time monitoring

# Timezone
EASTERN = pytz.timezone('US/Eastern') 