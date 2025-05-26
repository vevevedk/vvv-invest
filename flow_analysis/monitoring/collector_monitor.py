"""
Monitoring module for tracking collector status and health.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
import psycopg2
from psycopg2.extras import DictCursor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CollectorMonitor:
    """Monitor the status of news and darkpool collectors."""
    
    def __init__(self, db_config: Dict[str, str]):
        """Initialize the monitor with database configuration."""
        self.db_config = db_config
        self.collectors = {
            'news': {
                'name': 'News Collector',
                'interval': 300,  # 5 minutes
                'table': 'trading.news_headlines',
                'last_check': None,
                'status': 'unknown'
            },
            'darkpool': {
                'name': 'Darkpool Collector',
                'interval': 300,  # 5 minutes
                'table': 'trading.darkpool_trades',
                'last_check': None,
                'status': 'unknown'
            }
        }

    def check_collector_status(self, collector_type: str) -> Dict:
        """
        Check the status of a specific collector.
        
        Args:
            collector_type: Either 'news' or 'darkpool'
            
        Returns:
            Dict containing status information
        """
        if collector_type not in self.collectors:
            raise ValueError(f"Unknown collector type: {collector_type}")
            
        collector = self.collectors[collector_type]
        current_time = datetime.utcnow()
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    # Get the most recent record
                    cur.execute(f"""
                        SELECT collected_at 
                        FROM {collector['table']} 
                        ORDER BY collected_at DESC 
                        LIMIT 1
                    """)
                    result = cur.fetchone()
                    
                    if not result:
                        collector['status'] = 'no_data'
                        collector['last_check'] = current_time
                        return {
                            'status': 'no_data',
                            'message': f"No data found in {collector['table']}",
                            'last_update': None
                        }
                    
                    last_update = result['collected_at']
                    time_diff = (current_time - last_update).total_seconds()
                    
                    if time_diff > collector['interval'] * 2:
                        collector['status'] = 'stalled'
                        status = 'stalled'
                        message = f"Collector stalled. Last update: {last_update}"
                    elif time_diff > collector['interval']:
                        collector['status'] = 'delayed'
                        status = 'delayed'
                        message = f"Collector delayed. Last update: {last_update}"
                    else:
                        collector['status'] = 'running'
                        status = 'running'
                        message = f"Collector running. Last update: {last_update}"
                    
                    collector['last_check'] = current_time
                    return {
                        'status': status,
                        'message': message,
                        'last_update': last_update
                    }
                    
        except Exception as e:
            logger.error(f"Error checking {collector_type} collector status: {str(e)}")
            collector['status'] = 'error'
            collector['last_check'] = current_time
            return {
                'status': 'error',
                'message': f"Error checking status: {str(e)}",
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

    def get_collector_health(self) -> Dict:
        """
        Get the overall health status of all collectors.
        
        Returns:
            Dict containing health status and any issues
        """
        status = self.check_all_collectors()
        health = {
            'overall_status': 'healthy',
            'issues': [],
            'collectors': status
        }
        
        for collector_type, collector_status in status.items():
            if collector_status['status'] not in ['running', 'delayed']:
                health['overall_status'] = 'unhealthy'
                health['issues'].append({
                    'collector': collector_type,
                    'status': collector_status['status'],
                    'message': collector_status['message']
                })
        
        return health

def main():
    """Main function to demonstrate usage."""
    from flow_analysis.config.env_config import DB_CONFIG
    
    monitor = CollectorMonitor(DB_CONFIG)
    
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

if __name__ == "__main__":
    main() 