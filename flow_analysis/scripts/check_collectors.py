#!/usr/bin/env python3
"""
CLI tool to check the status of news and darkpool collectors.
"""

import sys
import argparse
from flow_analysis.monitoring.collector_monitor import CollectorMonitor
from flow_analysis.config.env_config import DB_CONFIG

def main():
    parser = argparse.ArgumentParser(description='Check collector status')
    parser.add_argument('--collector', choices=['news', 'darkpool', 'all'], 
                      default='all', help='Which collector to check')
    parser.add_argument('--json', action='store_true', 
                      help='Output in JSON format')
    
    args = parser.parse_args()
    
    monitor = CollectorMonitor(DB_CONFIG)
    
    if args.collector == 'all':
        health = monitor.get_collector_health()
    else:
        status = monitor.check_collector_status(args.collector)
        health = {
            'overall_status': 'healthy' if status['status'] in ['running', 'delayed'] else 'unhealthy',
            'issues': [] if status['status'] in ['running', 'delayed'] else [{
                'collector': args.collector,
                'status': status['status'],
                'message': status['message']
            }],
            'collectors': {args.collector: status}
        }
    
    if args.json:
        import json
        print(json.dumps(health, default=str, indent=2))
    else:
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
    
    # Exit with status code 1 if unhealthy
    sys.exit(0 if health['overall_status'] == 'healthy' else 1)

if __name__ == "__main__":
    main() 