#!/usr/bin/env python3

"""
Monitoring Module for News Collector
Handles metrics collection, health checks, and system monitoring
"""

import time
import logging
import psycopg2
import requests
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
from pathlib import Path
import pytz
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Constants
METRICS_WINDOW = 3600  # 1 hour in seconds
HEALTH_CHECK_INTERVAL = 300  # 5 minutes in seconds
QUEUE_SIZE = 1000
UW_BASE_URL = "https://api.unusualwhales.com"

@dataclass
class SystemMetrics:
    """System metrics data"""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    api_latency: float
    db_latency: float

@dataclass
class HealthStatus:
    """Health check status"""
    timestamp: datetime
    is_healthy: bool
    checks: Dict[str, bool]
    errors: List[str]

class MetricsCollector:
    """Collects and stores system metrics"""
    
    def __init__(self, db_config: Dict[str, str], api_token: str):
        self.db_config = db_config
        self.api_token = api_token
        self.logger = self._setup_logger()
        self.eastern = pytz.timezone('US/Eastern')
        self.engine = self._create_engine()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for metrics collection"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Add file handler
        file_handler = logging.FileHandler(log_dir / "metrics.log")
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        return logger

    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}",
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )

    def collect_metrics(self) -> SystemMetrics:
        """Collect current system metrics"""
        try:
            # CPU usage
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # Memory usage
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # Disk usage
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # API latency
            api_latency = self._measure_api_latency()
            
            # Database latency
            db_latency = self._measure_db_latency()
            
            return SystemMetrics(
                timestamp=datetime.now(self.eastern),
                cpu_usage=cpu_usage,
                memory_usage=memory_usage,
                disk_usage=disk_usage,
                api_latency=api_latency,
                db_latency=db_latency
            )
            
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {str(e)}")
            raise

    def _measure_api_latency(self) -> float:
        """Measure API endpoint latency"""
        try:
            start_time = time.time()
            response = requests.get(
                f"{UW_BASE_URL}/api/news/headlines",
                headers={"Authorization": f"Bearer {self.api_token}"},
                timeout=5
            )
            response.raise_for_status()
            return time.time() - start_time
        except Exception as e:
            self.logger.error(f"Error measuring API latency: {str(e)}")
            return -1.0

    def _measure_db_latency(self) -> float:
        """Measure database query latency"""
        try:
            start_time = time.time()
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return time.time() - start_time
        except Exception as e:
            self.logger.error(f"Error measuring database latency: {str(e)}")
            return -1.0

    def save_metrics(self, metrics: SystemMetrics) -> None:
        """Save metrics to database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO trading.system_metrics (
                            timestamp, cpu_usage, memory_usage,
                            disk_usage, api_latency, db_latency
                        ) VALUES (
                            :timestamp, :cpu_usage, :memory_usage,
                            :disk_usage, :api_latency, :db_latency
                        )
                    """),
                    {
                        'timestamp': metrics.timestamp,
                        'cpu_usage': metrics.cpu_usage,
                        'memory_usage': metrics.memory_usage,
                        'disk_usage': metrics.disk_usage,
                        'api_latency': metrics.api_latency,
                        'db_latency': metrics.db_latency
                    }
                )
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error saving metrics: {str(e)}")
            raise

class HealthChecker:
    """Performs system health checks"""
    
    def __init__(self, db_config: Dict[str, str], api_token: str):
        self.db_config = db_config
        self.api_token = api_token
        self.logger = self._setup_logger()
        self.eastern = pytz.timezone('US/Eastern')
        self.engine = self._create_engine()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for health checks"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Add file handler
        file_handler = logging.FileHandler(log_dir / "health.log")
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        return logger

    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}",
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )

    def check_health(self) -> HealthStatus:
        """Perform system health checks"""
        checks = {}
        errors = []
        
        # Check database connection
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            checks['database'] = True
        except Exception as e:
            checks['database'] = False
            errors.append(f"Database connection failed: {str(e)}")
            
        # Check API access
        try:
            response = requests.get(
                f"{UW_BASE_URL}/api/news/headlines",
                headers={"Authorization": f"Bearer {self.api_token}"},
                timeout=5
            )
            response.raise_for_status()
            checks['api'] = True
        except Exception as e:
            checks['api'] = False
            errors.append(f"API access failed: {str(e)}")
            
        # Check disk space
        try:
            disk = psutil.disk_usage('/')
            checks['disk_space'] = disk.percent < 90
            if not checks['disk_space']:
                errors.append(f"High disk usage: {disk.percent}%")
        except Exception as e:
            checks['disk_space'] = False
            errors.append(f"Disk space check failed: {str(e)}")
            
        # Check memory usage
        try:
            memory = psutil.virtual_memory()
            checks['memory'] = memory.percent < 90
            if not checks['memory']:
                errors.append(f"High memory usage: {memory.percent}%")
        except Exception as e:
            checks['memory'] = False
            errors.append(f"Memory check failed: {str(e)}")
            
        return HealthStatus(
            timestamp=datetime.now(self.eastern),
            is_healthy=all(checks.values()),
            checks=checks,
            errors=errors
        )

    def save_health_status(self, status: HealthStatus) -> None:
        """Save health check status to database"""
        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO trading.health_checks (
                            timestamp, is_healthy, checks, errors
                        ) VALUES (
                            :timestamp, :is_healthy, :checks, :errors
                        )
                    """),
                    {
                        'timestamp': status.timestamp,
                        'is_healthy': status.is_healthy,
                        'checks': json.dumps(status.checks),
                        'errors': json.dumps(status.errors)
                    }
                )
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error saving health status: {str(e)}")
            raise

def create_monitoring_tables(db_config: Dict[str, str]) -> None:
    """Create necessary monitoring tables"""
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@"
        f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    
    with engine.connect() as conn:
        # Create system metrics table
        conn.execute(text("""
            DROP TABLE IF EXISTS trading.system_metrics;
            
            CREATE TABLE trading.system_metrics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                cpu_usage FLOAT NOT NULL,
                memory_usage FLOAT NOT NULL,
                disk_usage FLOAT NOT NULL,
                api_latency FLOAT NOT NULL,
                db_latency FLOAT NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_system_metrics_timestamp 
            ON trading.system_metrics (timestamp);
        """))
        
        # Create health checks table
        conn.execute(text("""
            DROP TABLE IF EXISTS trading.health_checks;
            
            CREATE TABLE trading.health_checks (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                is_healthy BOOLEAN NOT NULL,
                checks JSONB NOT NULL,
                errors JSONB NOT NULL
            );
            
            CREATE INDEX IF NOT EXISTS idx_health_checks_timestamp 
            ON trading.health_checks (timestamp);
        """))
        
        conn.commit() 