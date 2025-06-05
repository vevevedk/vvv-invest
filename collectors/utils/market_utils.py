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
    Returns True if market is open, False otherwise.
    """
    # Get current time in US/Eastern timezone
    now = datetime.now(EASTERN)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        logger.info(f"Market is closed - Weekend: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return False
    
    # Check if it's a holiday
    if now.date() in [holiday.date() for holiday in MARKET_HOLIDAYS]:
        logger.info(f"Market is closed - Holiday: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return False
    
    # Check if it's during market hours
    current_time = now.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE

def get_next_market_open() -> datetime:
    """
    Calculate the next time the market will be open.
    Returns a datetime object in US/Eastern timezone.
    """
    now = datetime.now(EASTERN)
    
    # If market is open today, return today's open
    if now.time() < MARKET_OPEN and now.weekday() < 5:
        next_open = now.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute, second=0, microsecond=0)
        if next_open.date() not in [holiday.date() for holiday in MARKET_HOLIDAYS]:
            return next_open
    
    # Start checking from tomorrow
    days_ahead = 1
    while True:
        next_day = now + timedelta(days=days_ahead)
        if next_day.weekday() < 5 and next_day.date() not in [holiday.date() for holiday in MARKET_HOLIDAYS]:
            return next_day.replace(hour=MARKET_OPEN.hour, minute=MARKET_OPEN.minute, second=0, microsecond=0) 