import logging
from datetime import datetime, timedelta
import pytz
from typing import Optional

logger = logging.getLogger(__name__)

# Market hours (9:30 AM to 4:00 PM ET)
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# List of market holidays (add more as needed)
MARKET_HOLIDAYS = [
    # Add holidays here
]

def is_market_open() -> bool:
    """
    Check if the US stock market is currently open.
    Returns True if market is open, False otherwise.
    """
    # Get current time in US/Eastern timezone
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    # Check if it's a weekday (0 = Monday, 4 = Friday)
    if current_time.weekday() > 4:
        logger.info(f"Market is closed - Weekend: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return False
    
    # Check if it's a holiday (you can add more holidays as needed)
    holidays = [
        "2024-01-01",  # New Year's Day
        "2024-01-15",  # Martin Luther King Jr. Day
        "2024-02-19",  # Presidents Day
        "2024-03-29",  # Good Friday
        "2024-05-27",  # Memorial Day
        "2024-06-19",  # Juneteenth
        "2024-07-04",  # Independence Day
        "2024-09-02",  # Labor Day
        "2024-11-28",  # Thanksgiving Day
        "2024-12-25",  # Christmas Day
    ]
    
    if current_time.strftime("%Y-%m-%d") in holidays:
        logger.info(f"Market is closed - Holiday: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return False
    
    # Check if it's during market hours (9:30 AM - 4:00 PM ET)
    market_open = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = current_time.replace(hour=16, minute=0, second=0, microsecond=0)
    
    is_open = market_open <= current_time <= market_close
    logger.info(f"Current time: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}, Market open: {is_open}")
    return is_open

def get_next_market_open() -> datetime:
    """
    Calculate the next time the market will be open.
    Returns a datetime object in US/Eastern timezone.
    """
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    
    # If current time is before market open today
    market_open_today = current_time.replace(hour=9, minute=30, second=0, microsecond=0)
    if current_time < market_open_today and current_time.weekday() <= 4:
        return market_open_today
    
    # Start checking from tomorrow
    next_day = current_time + timedelta(days=1)
    while True:
        # Skip weekends
        if next_day.weekday() > 4:
            next_day += timedelta(days=1)
            continue
            
        # Skip holidays
        holidays = [
            "2024-01-01",  # New Year's Day
            "2024-01-15",  # Martin Luther King Jr. Day
            "2024-02-19",  # Presidents Day
            "2024-03-29",  # Good Friday
            "2024-05-27",  # Memorial Day
            "2024-06-19",  # Juneteenth
            "2024-07-04",  # Independence Day
            "2024-09-02",  # Labor Day
            "2024-11-28",  # Thanksgiving Day
            "2024-12-25",  # Christmas Day
        ]
        
        if next_day.strftime("%Y-%m-%d") in holidays:
            next_day += timedelta(days=1)
            continue
            
        # Found next trading day, return market open time
        return next_day.replace(hour=9, minute=30, second=0, microsecond=0) 