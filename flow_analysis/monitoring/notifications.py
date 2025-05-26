"""
Slack notification system for collector monitoring.
"""

import os
import logging
import requests
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class SlackNotifier:
    """Handle Slack notifications for collector monitoring."""
    
    def __init__(self, webhook_url: Optional[str] = None):
        """Initialize with Slack webhook URL."""
        self.webhook_url = webhook_url or os.getenv('SLACK_WEBHOOK_URL')
        if not self.webhook_url:
            logger.warning("No Slack webhook URL provided. Notifications will be disabled.")
    
    def _send_message(self, message: str, color: str = "good") -> bool:
        """Send a message to Slack."""
        if not self.webhook_url:
            return False
            
        try:
            payload = {
                "attachments": [{
                    "color": color,
                    "text": message,
                    "ts": datetime.utcnow().timestamp()
                }]
            }
            response = requests.post(self.webhook_url, json=payload)
            response.raise_for_status()
            return True
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}")
            return False
    
    def notify_collector_status(self, collector_type: str, status: Dict) -> bool:
        """Notify about collector status changes."""
        if status['status'] == 'running':
            return self._send_message(
                f"✅ {collector_type.title()} Collector is running\n"
                f"Last update: {status['last_update']}"
            )
        elif status['status'] == 'delayed':
            return self._send_message(
                f"⚠️ {collector_type.title()} Collector is delayed\n"
                f"Last update: {status['last_update']}",
                color="warning"
            )
        elif status['status'] == 'stalled':
            return self._send_message(
                f"🚨 {collector_type.title()} Collector has stalled!\n"
                f"Last update: {status['last_update']}",
                color="danger"
            )
        elif status['status'] == 'error':
            return self._send_message(
                f"❌ {collector_type.title()} Collector encountered an error!\n"
                f"Error: {status['message']}",
                color="danger"
            )
        return False
    
    def notify_health_status(self, health: Dict) -> bool:
        """Notify about overall system health."""
        if health['overall_status'] == 'healthy':
            return self._send_message(
                "✅ All collectors are healthy",
                color="good"
            )
        else:
            issues = "\n".join([
                f"• {issue['collector']}: {issue['message']}"
                for issue in health['issues']
            ])
            return self._send_message(
                f"🚨 System Health Alert!\n\nIssues:\n{issues}",
                color="danger"
            ) 