import logging
from datetime import datetime, time, timedelta
import pytz
from typing import Optional

logger = logging.getLogger(__name__)

# Market hours (9:30 AM to 4:00 PM ET)
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

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

# Timezone
EASTERN = pytz.timezone('US/Eastern')

def is_market_open() -> bool:
    """
    Check if the US stock market is currently open.
    Market hours: 9:30 AM - 4:00 PM Eastern Time, Monday-Friday
    """
    # Get current time in Eastern Time
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    # Check if it's a weekday
    if current_time.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        return False
    
    # Check if it's within market hours
    market_open = time(9, 30)  # 9:30 AM
    market_close = time(16, 0)  # 4:00 PM
    current_time_et = current_time.time()
    
    return market_open <= current_time_et <= market_close

def get_next_market_open() -> datetime:
    """Get the next time the market will open."""
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    # If it's after market close, move to next day
    if current_time.time() > time(16, 0):
        current_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0) + timedelta(days=1)
    
    # If it's before market open, use today
    if current_time.time() < time(9, 30):
        current_time = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
    
    # If it's weekend, move to next Monday
    while current_time.weekday() >= 5:
        current_time = current_time + timedelta(days=1)
    
    return current_time

def get_market_status() -> dict:
    """Get detailed market status information."""
    is_open = is_market_open()
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    status = {
        "is_market_open": is_open,
        "current_time_et": current_time.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "next_market_open": get_next_market_open().strftime("%Y-%m-%d %H:%M:%S %Z")
    }
    
    return status 