import logging
import sys
from datetime import datetime
from pathlib import Path

class DenseFormatter(logging.Formatter):
    """Custom formatter for dense, concise logging."""
    
    def format(self, record):
        # Remove redundant timestamp if it's already in the message
        if hasattr(record, 'timestamp'):
            record.msg = f"{record.msg}"
        
        # Format the message
        if record.levelno >= logging.ERROR:
            # For errors, include more detail
            return f"❌ {record.levelname}: {record.msg}"
        elif record.levelno >= logging.WARNING:
            return f"⚠️ {record.msg}"
        else:
            # For info messages, keep it concise
            return f"ℹ️ {record.msg}"

def setup_logging(name: str, log_file: str = None) -> logging.Logger:
    """Set up logging with custom formatters and handlers."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create formatters
    dense_formatter = DenseFormatter()
    
    # Console handler with dense formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(dense_formatter)
    logger.addHandler(console_handler)
    
    # File handler with detailed formatting
    if log_file:
        log_path = Path('logs/collector')
        log_path.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_path / log_file)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_collector_summary(collector_name, start_time, end_time, items_collected, api_credits_used=0):
    """Log a summary of the collection process."""
    logger = logging.getLogger(__name__)
    duration = (datetime.now() - start_time).total_seconds()
    
    logger.info(f"\n{collector_name} Collection Summary:")
    logger.info("=" * 50)
    logger.info(f"Time Range: {start_time} to {end_time}")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"Items Collected: {items_collected}")
    if api_credits_used > 0:
        logger.info(f"API Credits Used: {api_credits_used}")
    logger.info("=" * 50) 