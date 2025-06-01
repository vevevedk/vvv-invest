import logging
from datetime import datetime, time
import pytz

logger = logging.getLogger(__name__)

def is_market_open() -> bool:
    """Check if the market is currently open."""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    # Market hours (9:30 AM to 4:00 PM ET)
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    # Check if it's a weekday
    if now.weekday() >= 5:  # 5 is Saturday, 6 is Sunday
        logger.info("Market is closed - weekend")
        return False
    
    # Check if it's a holiday (you might want to add a holiday calendar)
    # For now, we'll just check regular hours
    
    current_time = now.time()
    is_open = market_open <= current_time <= market_close
    
    if not is_open:
        logger.info(f"Market is closed - current ET time: {now.strftime('%H:%M:%S')}")
    
    return is_open 