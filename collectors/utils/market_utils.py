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

# Collector hours (pre-market, regular, after-hours)
COLLECTOR_PRE_MARKET_OPEN = time(4, 0)
COLLECTOR_REGULAR_OPEN = time(9, 30)
COLLECTOR_REGULAR_CLOSE = time(16, 0)
COLLECTOR_AFTER_HOURS_CLOSE = time(20, 0)

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

def is_collector_open() -> bool:
    """
    Check if the collector should be running (pre-market, regular, or after-hours).
    """
    eastern = pytz.timezone('US/Eastern')
    current_time = datetime.now(eastern)
    if current_time.weekday() >= 5:
        return False
    current_time_et = current_time.time()
    # Pre-market
    if COLLECTOR_PRE_MARKET_OPEN <= current_time_et < COLLECTOR_REGULAR_OPEN:
        return True
    # Regular
    if COLLECTOR_REGULAR_OPEN <= current_time_et < COLLECTOR_REGULAR_CLOSE:
        return True
    # After-hours
    if COLLECTOR_REGULAR_CLOSE <= current_time_et < COLLECTOR_AFTER_HOURS_CLOSE:
        return True
    return False

def get_next_collector_open() -> datetime:
    """
    Get the next time the collector will run (pre-market, regular, or after-hours).
    """
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    today = now.date()
    # If it's weekend, move to next Monday
    while now.weekday() >= 5:
        now += timedelta(days=1)
        today = now.date()
    t = now.time()
    # If before pre-market
    if t < COLLECTOR_PRE_MARKET_OPEN:
        return now.replace(hour=COLLECTOR_PRE_MARKET_OPEN.hour, minute=0, second=0, microsecond=0)
    # If in pre-market, next open is regular
    if COLLECTOR_PRE_MARKET_OPEN <= t < COLLECTOR_REGULAR_OPEN:
        return now.replace(hour=COLLECTOR_REGULAR_OPEN.hour, minute=COLLECTOR_REGULAR_OPEN.minute, second=0, microsecond=0)
    # If in regular, next open is after-hours
    if COLLECTOR_REGULAR_OPEN <= t < COLLECTOR_REGULAR_CLOSE:
        return now.replace(hour=COLLECTOR_REGULAR_CLOSE.hour, minute=0, second=0, microsecond=0)
    # If in after-hours, next open is tomorrow's pre-market
    if COLLECTOR_REGULAR_CLOSE <= t < COLLECTOR_AFTER_HOURS_CLOSE:
        return (now + timedelta(days=1)).replace(hour=COLLECTOR_PRE_MARKET_OPEN.hour, minute=0, second=0, microsecond=0)
    # If after after-hours, next open is tomorrow's pre-market
    if t >= COLLECTOR_AFTER_HOURS_CLOSE:
        return (now + timedelta(days=1)).replace(hour=COLLECTOR_PRE_MARKET_OPEN.hour, minute=0, second=0, microsecond=0)
    # Fallback
    return now.replace(hour=COLLECTOR_PRE_MARKET_OPEN.hour, minute=0, second=0, microsecond=0)

def get_market_status() -> dict:
    """
    Get detailed market status information, including collector open status and next collector open time.
    """
    is_open = is_market_open()
    is_collector = is_collector_open()
    eastern = pytz.timezone('US/Eastern')
    cest = pytz.timezone('Europe/Copenhagen')
    current_time_et = datetime.now(eastern)
    next_market_open_et = get_next_market_open()
    next_collector_open_et = get_next_collector_open()
    # Convert to CEST
    current_time_cest = current_time_et.astimezone(cest)
    next_market_open_cest = next_market_open_et.astimezone(cest)
    next_collector_open_cest = next_collector_open_et.astimezone(cest)
    status = {
        "is_market_open": is_open,
        "is_collector_open": is_collector,
        "current_time_et": current_time_et.isoformat(),
        "next_market_open_et": next_market_open_et.isoformat(),
        "next_collector_open_et": next_collector_open_et.isoformat(),
        "current_time_cest": current_time_cest.isoformat(),
        "next_market_open_cest": next_market_open_cest.isoformat(),
        "next_collector_open_cest": next_collector_open_cest.isoformat(),
        "timezone_et": "US/Eastern",
        "timezone_cest": "Europe/Copenhagen (CEST)"
    }
    return status 