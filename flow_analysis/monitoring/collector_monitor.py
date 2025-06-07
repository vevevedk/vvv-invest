"""
Monitoring module for tracking collector status and health.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any
import psycopg2
from psycopg2.extras import DictCursor
from config.db_config import get_db_config
from flow_analysis.config.env_config import DB_CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CollectorMonitor:
    """Monitor the status of news and darkpool collectors."""
    
    def __init__(self, db_config: Dict[str, Any]):
        """Initialize the monitor with database configuration."""
        self.db_config = db_config
        self.collectors = ['darkpool', 'news']  # Add more collectors as needed
        self.heartbeat_timeout = timedelta(minutes=5)  # Consider collector dead after 5 minutes
        self.stall_threshold = timedelta(minutes=15)  # Consider collector stalled after 15 minutes

    def check_collector_status(self, collector_type: str) -> Dict:
        """
        Check the status of a specific collector using enhanced logging information.
        
        Args:
            collector_type: Either 'news' or 'darkpool'
            
        Returns:
            Dict containing status information
        """
        if collector_type not in self.collectors:
            raise ValueError(f"Unknown collector type: {collector_type}")
            
        collector = self.collectors[collector_type]
        current_time = datetime.now(timezone.utc)
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    # First check for recent heartbeats
                    cur.execute("""
                        SELECT timestamp, status, details
                        FROM trading.collector_logs
                        WHERE collector_name = %s
                        AND is_heartbeat = true
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (collector_type,))
                    heartbeat = cur.fetchone()
                    
                    # Then check for recent data collection
                    column = 'collection_time' if collector_type == 'darkpool' else 'collected_at'
                    cur.execute(f"""
                        SELECT {column} 
                        FROM {collector['table']} 
                        ORDER BY {column} DESC 
                        LIMIT 1
                    """)
                    data_result = cur.fetchone()
                    
                    # Check for recent errors
                    cur.execute("""
                        SELECT timestamp, message, error_details
                        FROM trading.collector_logs
                        WHERE collector_name = %s
                        AND level = 'ERROR'
                        AND timestamp > NOW() - INTERVAL '1 hour'
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """, (collector_type,))
                    error = cur.fetchone()
                    
                    # Determine status based on all available information
                    status_info = self._determine_status(
                        collector_type,
                        heartbeat,
                        data_result,
                        error,
                        current_time
                    )
                    
                    collector['status'] = status_info['status']
                    collector['last_check'] = current_time
                    
                    return status_info
                    
        except Exception as e:
            logger.error(f"Error checking {collector_type} collector status: {str(e)}")
            collector['status'] = 'error'
            collector['last_check'] = current_time
            return {
                'status': 'error',
                'message': f"Error checking status: {str(e)}",
                'last_update': None,
                'error_details': {'error': str(e)}
            }

    def _determine_status(
        self,
        collector_type: str,
        heartbeat: Optional[Dict],
        data_result: Optional[Dict],
        error: Optional[Dict],
        current_time: datetime
    ) -> Dict:
        """Determine collector status based on all available information."""
        
        # If there's a recent error, that takes precedence
        if error and (current_time - error['timestamp']).total_seconds() < 300:  # 5 minutes
            return {
                'status': 'error',
                'message': error['message'],
                'last_update': error['timestamp'],
                'error_details': error['error_details']
            }
        
        # Check heartbeat status
        if heartbeat:
            heartbeat_age = (current_time - heartbeat['timestamp']).total_seconds()
            if heartbeat_age > self.collectors[collector_type]['heartbeat_interval'] * 2:
                return {
                    'status': 'stalled',
                    'message': f"No recent heartbeat. Last seen: {heartbeat['timestamp']}",
                    'last_update': heartbeat['timestamp'],
                    'details': heartbeat['details']
                }
            
            # If heartbeat indicates waiting for market open
            if heartbeat['status'] == 'waiting_for_market_open':
                return {
                    'status': 'waiting_for_market_open',
                    'message': 'Waiting for market to open',
                    'last_update': heartbeat['timestamp'],
                    'details': heartbeat['details']
                }
        
        # Check data collection status
        if data_result:
            last_update = data_result['collection_time' if collector_type == 'darkpool' else 'collected_at']
            if last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=timezone.utc)
            
            time_diff = (current_time - last_update).total_seconds()
            
            if time_diff > self.collectors[collector_type]['interval'] * 2:
                return {
                    'status': 'stalled',
                    'message': f"Data collection stalled. Last update: {last_update}",
                    'last_update': last_update
                }
            elif time_diff > self.collectors[collector_type]['interval']:
                return {
                    'status': 'delayed',
                    'message': f"Data collection delayed. Last update: {last_update}",
                    'last_update': last_update
                }
            else:
                return {
                    'status': 'running',
                    'message': f"Collector running. Last update: {last_update}",
                    'last_update': last_update
                }
        
        # If no data but has heartbeat
        if heartbeat:
            return {
                'status': 'no_data',
                'message': f"Collector alive but no data collected. Last heartbeat: {heartbeat['timestamp']}",
                'last_update': heartbeat['timestamp'],
                'details': heartbeat['details']
            }
        
        # If no data and no heartbeat
        return {
            'status': 'unknown',
            'message': "No recent activity detected",
            'last_update': None
        }

    def check_all_collectors(self) -> Dict:
        """
        Check the status of all collectors.
        
        Returns:
            Dict containing status information for all collectors
        """
        results = {}
        for collector_type in self.collectors:
            results[collector_type] = self.check_collector_status(collector_type)
        return results

    def get_collector_health(self) -> Dict[str, Any]:
        """
        Get the health status of all collectors.
        
        Returns:
            Dictionary with overall health status and individual collector statuses
        """
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get latest status for each collector
                    cur.execute("""
                        WITH latest_logs AS (
                            SELECT DISTINCT ON (collector_name)
                                collector_name,
                                timestamp,
                                level,
                                message,
                                task_type,
                                details,
                                is_heartbeat,
                                status,
                                error_details
                            FROM trading.collector_logs
                            ORDER BY collector_name, timestamp DESC
                        )
                        SELECT * FROM latest_logs
                    """)
                    latest_logs = cur.fetchall()
                    
                    # Get latest heartbeat for each collector
                    cur.execute("""
                        WITH latest_heartbeats AS (
                            SELECT DISTINCT ON (collector_name)
                                collector_name,
                                timestamp
                            FROM trading.collector_logs
                            WHERE is_heartbeat = true
                            ORDER BY collector_name, timestamp DESC
                        )
                        SELECT * FROM latest_heartbeats
                    """)
                    latest_heartbeats = {row[0]: row[1] for row in cur.fetchall()}
                    
                    # Process collector statuses
                    collector_statuses = {}
                    for log in latest_logs:
                        collector_name = log[0]
                        timestamp = log[1]
                        level = log[2]
                        message = log[3]
                        task_type = log[4]
                        details = log[5]
                        is_heartbeat = log[6]
                        status = log[7]
                        error_details = log[8]
                        
                        # Get latest heartbeat time
                        last_heartbeat = latest_heartbeats.get(collector_name)
                        
                        # Determine collector status
                        if error_details:
                            status = 'error'
                        elif not last_heartbeat:
                            status = 'no_data'
                        elif datetime.utcnow() - last_heartbeat > self.heartbeat_timeout:
                            status = 'stalled'
                        elif status == 'running' and datetime.utcnow() - timestamp > self.stall_threshold:
                            status = 'delayed'
                        
                        collector_statuses[collector_name] = {
                            'status': status,
                            'last_update': timestamp.isoformat(),
                            'message': message,
                            'error_details': error_details
                        }
                    
                    # Determine overall health
                    overall_status = 'healthy'
                    if any(s['status'] in ['error', 'stalled'] for s in collector_statuses.values()):
                        overall_status = 'unhealthy'
                    elif any(s['status'] == 'delayed' for s in collector_statuses.values()):
                        overall_status = 'degraded'
                    
                    return {
                        'overall_status': overall_status,
                        'collectors': collector_statuses
                    }
                    
        except Exception as e:
            return {
                'overall_status': 'error',
                'error': str(e),
                'collectors': {}
            }

    def get_collector_history(self, collector_name: str, hours: int = 24) -> List[Dict[str, Any]]:
        """
        Get historical status information for a collector.
        
        Args:
            collector_name: Name of the collector
            hours: Number of hours of history to retrieve
            
        Returns:
            List of status records
        """
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            timestamp,
                            level,
                            message,
                            task_type,
                            details,
                            is_heartbeat,
                            status,
                            error_details
                        FROM trading.collector_logs
                        WHERE collector_name = %s
                        AND timestamp > NOW() - INTERVAL '%s hours'
                        ORDER BY timestamp ASC
                    """, (collector_name, hours))
                    
                    return [{
                        'timestamp': row[0].isoformat(),
                        'level': row[1],
                        'message': row[2],
                        'task_type': row[3],
                        'details': row[4],
                        'is_heartbeat': row[5],
                        'status': row[6],
                        'error_details': row[7]
                    } for row in cur.fetchall()]
                    
        except Exception as e:
            return []

def main():
    """Main function to demonstrate usage."""
    monitor = CollectorMonitor(get_db_config())
    
    # Check all collectors
    health = monitor.get_collector_health()
    
    # Print status
    print("\nCollector Health Status:")
    print("-" * 50)
    print(f"Overall Status: {health['overall_status']}")
    
    if health['issues']:
        print("\nIssues Found:")
        for issue in health['issues']:
            print(f"- {issue['collector']}: {issue['message']}")
    
    print("\nDetailed Status:")
    for collector_type, status in health['collectors'].items():
        print(f"\n{collector_type.title()} Collector:")
        print(f"Status: {status['status']}")
        print(f"Message: {status['message']}")
        if status.get('last_update'):
            print(f"Last Update: {status['last_update']}")

if __name__ == "__main__":
    main() 