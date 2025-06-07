"""
Logging configuration for collectors.
"""

import logging
import json
import os
from datetime import datetime
import psycopg2
from flow_analysis.config.env_config import DB_CONFIG

def setup_logging(collector_name: str, log_file: str = None) -> logging.Logger:
    """
    Set up logging for a collector.
    
    Args:
        collector_name: Name of the collector (e.g., 'darkpool', 'news')
        log_file: Optional path to log file. If None, logs only to database.
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(collector_name)
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    logger.handlers = []
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger

def log_to_db(collector_name: str, level: str, message: str, task_type: str = None, 
              details: dict = None, is_heartbeat: bool = False, status: str = None,
              error_details: dict = None):
    """
    Log a message to the database.
    
    Args:
        collector_name: Name of the collector
        level: Log level (INFO, WARNING, ERROR)
        message: Log message
        task_type: Type of task being performed
        details: Additional details as a dictionary
        is_heartbeat: Whether this is a heartbeat message
        status: Current collector status
        error_details: Error details if this is an error message
    """
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading.collector_logs (
                        timestamp,
                        collector_name,
                        level,
                        message,
                        task_type,
                        details,
                        is_heartbeat,
                        status,
                        error_details
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    datetime.utcnow(),
                    collector_name,
                    level,
                    message,
                    task_type,
                    json.dumps(details) if details else None,
                    is_heartbeat,
                    status,
                    json.dumps(error_details) if error_details else None
                ))
                conn.commit()
    except Exception as e:
        # Fallback to console logging if DB logging fails
        print(f"Failed to log to database: {e}")
        print(f"Collector: {collector_name}, Level: {level}, Message: {message}")

def log_heartbeat(collector_name: str, status: str = None, message: str = None):
    """
    Log a heartbeat message.
    
    Args:
        collector_name: Name of the collector
        status: Current collector status
        message: Optional message to include
    """
    if message is None:
        message = f"Collector heartbeat at {datetime.utcnow().isoformat()}"
    
    log_to_db(
        collector_name=collector_name,
        level='INFO',
        message=message,
        task_type='heartbeat',
        is_heartbeat=True,
        status=status
    )

def log_collector_summary(collector_name: str, start_time: datetime, end_time: datetime,
                         items_collected: int, api_credits_used: int = None,
                         task_type: str = 'collection', status: str = 'completed',
                         error_details: dict = None):
    """
    Log a summary of a collection run.
    
    Args:
        collector_name: Name of the collector
        start_time: When the collection started
        end_time: When the collection ended
        items_collected: Number of items collected
        api_credits_used: Number of API credits used
        task_type: Type of task (e.g., 'collection', 'backfill')
        status: Status of the collection
        error_details: Error details if the collection failed
    """
    duration = (end_time - start_time).total_seconds()
    
    details = {
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'duration_seconds': duration,
        'items_collected': items_collected,
        'api_credits_used': api_credits_used
    }
    
    level = 'ERROR' if error_details else 'INFO'
    message = f"Collection {'failed' if error_details else 'completed'}: {items_collected} items in {duration:.1f}s"
    
    log_to_db(
        collector_name=collector_name,
        level=level,
        message=message,
        task_type=task_type,
        details=details,
        status=status,
        error_details=error_details
    )

def log_error(collector_name: str, error: Exception, task_type: str = None,
              details: dict = None, status: str = 'error'):
    """
    Log an error.
    
    Args:
        collector_name: Name of the collector
        error: The exception that occurred
        task_type: Type of task being performed
        details: Additional details about the error
        status: Status to set for the collector
    """
    error_details = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'traceback': traceback.format_exc()
    }
    
    if details:
        error_details.update(details)
    
    log_to_db(
        collector_name=collector_name,
        level='ERROR',
        message=str(error),
        task_type=task_type,
        details=details,
        status=status,
        error_details=error_details
    )

def log_warning(collector_name: str, message: str, task_type: str = None,
                details: dict = None, status: str = None):
    """
    Log a warning.
    
    Args:
        collector_name: Name of the collector
        message: Warning message
        task_type: Type of task being performed
        details: Additional details about the warning
        status: Status to set for the collector
    """
    log_to_db(
        collector_name=collector_name,
        level='WARNING',
        message=message,
        task_type=task_type,
        details=details,
        status=status
    )

def log_info(collector_name: str, message: str, task_type: str = None,
             details: dict = None, status: str = None):
    """
    Log an info message.
    
    Args:
        collector_name: Name of the collector
        message: Info message
        task_type: Type of task being performed
        details: Additional details
        status: Status to set for the collector
    """
    log_to_db(
        collector_name=collector_name,
        level='INFO',
        message=message,
        task_type=task_type,
        details=details,
        status=status
    ) 