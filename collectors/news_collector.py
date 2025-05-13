import logging
import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from collectors.base_collector import BaseCollector
from flow_analysis.scripts.news_collector import NewsCollector as UWNewsCollector
from flow_analysis.config.api_config import UW_API_TOKEN

logger = logging.getLogger(__name__)

class NewsCollector(BaseCollector):
    """Collects news articles from UW API."""

    def __init__(self):
        super().__init__()
        if not UW_API_TOKEN:
            raise ValueError("UW_API_TOKEN environment variable is not set")
        self.uw_collector = UWNewsCollector()

    def collect(self):
        """Collect news articles from UW API."""
        try:
            self.logger.info("Starting news collection...")
            self.uw_collector.run()
            self.logger.info("News collector completed successfully")
        except Exception as e:
            self.logger.error(f"Error in news collection: {str(e)}", exc_info=True)
            raise 