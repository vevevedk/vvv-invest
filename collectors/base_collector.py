import logging

class BaseCollector:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        try:
            self.logger.info("Starting collector")
            self.collect()
        except Exception as e:
            self.logger.error(f"Collector failed: {e}", exc_info=True)
        finally:
            self.cleanup()

    def collect(self):
        raise NotImplementedError

    def cleanup(self):
        self.logger.info("Cleanup complete") 